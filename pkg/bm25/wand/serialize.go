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

package wand

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Tar member names (cuVS-style multi-member archive).
const (
	memberDocmap   = "docmap"   // pkType + ord -> pk value
	memberTermDict = "termdict" // out-of-dict term -> overflow word-id
	memberWandIdx  = "wandidx"  // version + word-id -> ranking byte offset (resident index)
	memberWandRank = "wandrank" // per-term self-contained block-max directory (mmap view, lazy)
	memberWandBlk  = "wandblk"  // per-term docID/tf blocks (delta+varint; mmap view, block-decoded)
)

// wandFormatV1 is the block-compressed WAND on-disk format: a RANKING directory of
// per-term self-contained Block-Max skip entries reachable by byte offset (the
// wandidx maps word-id -> that offset), plus a BLOCKS section of per-BlockSize-doc
// docID gaps (delta+varint) + raw tfs. Delta+varint shrinks docIDs ~4× and blocking
// them lets the loader keep them on the mmap and random-access one block, instead of
// the old flat, fully-resident postings. This is a fork of fulltext2's
// postingsFormatV6 with the positions section dropped (bm25 is position-free) and
// the FST replaced by the word-id->offset index. The format is free to break (no
// migration); a stale blob is rejected by this version byte.
const wandFormatV1 byte = 1

func log10(x float64) float64 { return math.Log10(x) }

// Checksum returns the CRC32 (IEEE) of the serialized bytes.
func Checksum(b []byte) uint32 { return crc32.ChecksumIEEE(b) }

// Serialize encodes the model into a tar archive of three members.
func (m *WandModel) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	docmap, err := m.encodeDocmap()
	if err != nil {
		return nil, err
	}
	if err := writeMember(tw, memberDocmap, docmap); err != nil {
		return nil, err
	}
	if err := writeMember(tw, memberTermDict, m.encodeTermDict()); err != nil {
		return nil, err
	}
	idx, ranking, blocks := m.encodeWand()
	if err := writeMember(tw, memberWandIdx, idx); err != nil {
		return nil, err
	}
	if err := writeMember(tw, memberWandRank, ranking); err != nil {
		return nil, err
	}
	if err := writeMember(tw, memberWandBlk, blocks); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize parses a tar archive produced by Serialize into a LOADED, queryable
// model: the small docmap / termdict members expand resident, while the ranking
// directory + docID/tf blocks stay as VIEWS into the read blob (decoded per term on
// demand, NOT expanded at load). This is the in-memory path (CDC tail frames, tests
// via bytes.Reader); the base-index path (LoadFromStorage) mmaps the file and binds
// the same views to it. The returned model retains the read bytes (ranking/blocks
// slice into them), so the blob is kept alive by GC. For a multi-GB base do NOT use
// this (it would ReadAll onto the Go heap) — use the mmap loader.
func Deserialize(id string, r io.Reader) (*WandModel, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return decodeLoaded(id, data)
}

// decodeLoaded builds a LOADED model over data (an in-memory blob or an mmap): the
// docmap/termdict expand resident, the ranking directory + blocks are bound as views
// into data, and per-term entries decode lazily (lookupTerm). Callers keep data alive
// (mmapData for a base, the retained slice/GC for a tail).
func decodeLoaded(id string, data []byte) (*WandModel, error) {
	m := NewWandModel(id, 0)
	docmap, termdict, idx, ranking, blocks, err := sliceMembers(data)
	if err != nil {
		return nil, err
	}
	if err := m.decodeDocmap(docmap); err != nil {
		return nil, err
	}
	if err := m.decodeTermDict(termdict); err != nil {
		return nil, err
	}
	if err := m.bindWand(idx, ranking, blocks); err != nil {
		return nil, err
	}
	m.N = int64(len(m.pks))
	m.computeAvgDocLen() // per-term Block-Max stats come from disk (decodeTermEntry), lazily
	return m, nil
}

// sliceMembers returns the docmap / termdict / wandidx / wandrank / wandblk members
// as SLICES into data (zero-copy) by walking the tar and tracking the reader offset,
// so the same code serves an in-memory blob or an mmap'd file.
func sliceMembers(data []byte) (docmap, termdict, idx, ranking, blocks []byte, err error) {
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
			return nil, nil, nil, nil, nil, moerr.NewInternalErrorNoCtx("wand: member out of range")
		}
		seg := data[off : off+h.Size]
		switch h.Name {
		case memberDocmap:
			docmap = seg
		case memberTermDict:
			termdict = seg
		case memberWandIdx:
			idx = seg
		case memberWandRank:
			ranking = seg
		case memberWandBlk:
			blocks = seg
		}
	}
	return docmap, termdict, idx, ranking, blocks, nil
}

func writeMember(tw *tar.Writer, name string, data []byte) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// leBuf appends little-endian scalars to a bytes.Buffer WITHOUT the per-call heap
// allocation binary.Write incurs: binary.Write's `data any` parameter boxes every
// scalar to the heap, which in the per-term / per-doc serialize loops is millions
// of tiny garbage allocations. PutUintXX into the reused tmp array avoids it (the
// bytes are byte-identical to binary.Write(LittleEndian, ...), so the on-disk
// format is unchanged). Slice writes still go through binary.Write (one buffer
// alloc, no per-element boxing).
type leBuf struct {
	b   bytes.Buffer
	tmp [binary.MaxVarintLen64]byte
}

func (w *leBuf) u32(v uint32) { binary.LittleEndian.PutUint32(w.tmp[:4], v); w.b.Write(w.tmp[:4]) }
func (w *leBuf) u64(v uint64) { binary.LittleEndian.PutUint64(w.tmp[:8], v); w.b.Write(w.tmp[:8]) }
func (w *leBuf) i32(v int32)  { w.u32(uint32(v)) }
func (w *leBuf) i64(v int64)  { w.u64(uint64(v)) }

// uvarint appends v as a LEB128 varint (1–10 bytes) — used for the delta-encoded
// docID gaps and the block directory, where values are small.
func (w *leBuf) uvarint(v uint64) { n := binary.PutUvarint(w.tmp[:], v); w.b.Write(w.tmp[:n]) }

// encodePkLen writes a length-prefixed pk directly into the buffer — byte-identical
// to `binary.Write(len); Write(encodePk(...))` but without encodePk's per-pk small
// allocation for integer keys. Keep the type switch in sync with encodePk.
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
		// Any other type encodePk handles (e.g. uuid, stored as text) — no integer
		// fast path, so length-prefix its encodePk bytes.
		pkb, err := encodePk(pkType, v)
		if err != nil {
			return err
		}
		w.u32(uint32(len(pkb)))
		w.b.Write(pkb)
	}
	return nil
}

// ---- docmap: pkType + ord -> pk ----

func (m *WandModel) encodeDocmap() ([]byte, error) {
	var w leBuf
	w.i32(m.PkType)
	w.i64(int64(len(m.pks)))
	for _, pk := range m.pks {
		if err := w.encodePkLen(m.PkType, pk); err != nil {
			return nil, err
		}
	}
	// per-doc length (ord-aligned with pks), for BM25. Zero-copy LE bytes (host is
	// little-endian — decodeWand reads it back the same way with UnsafeSliceCast);
	// avoids binary.Write's temp buffer + the io.Writer boxing that heap-allocates w.
	w.b.Write(util.UnsafeSliceToBytes(m.docLen))
	return w.b.Bytes(), nil
}

func (m *WandModel) decodeDocmap(data []byte) error {
	r := bytes.NewReader(data)
	if err := binary.Read(r, binary.LittleEndian, &m.PkType); err != nil {
		return err
	}
	var n int64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return err
	}
	m.pks = make([]any, n)
	for i := int64(0); i < n; i++ {
		var l uint32
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return err
		}
		raw := make([]byte, l)
		if _, err := io.ReadFull(r, raw); err != nil {
			return err
		}
		v, err := decodePk(m.PkType, raw)
		if err != nil {
			return err
		}
		m.pks[i] = v
	}
	m.docLen = make([]int32, n)
	if err := binary.Read(r, binary.LittleEndian, m.docLen); err != nil {
		return err
	}
	return nil
}

// ---- termdict: overflow term -> word-id ----

func (m *WandModel) encodeTermDict() []byte {
	var w leBuf
	w.i64(int64(len(m.overflow)))
	terms := make([]string, 0, len(m.overflow))
	for t := range m.overflow {
		terms = append(terms, t)
	}
	sort.Strings(terms) // deterministic output
	for _, term := range terms {
		w.u32(uint32(len(term)))
		w.b.WriteString(term) // WriteString avoids the []byte(term) copy
		w.i32(m.overflow[term])
	}
	return w.b.Bytes()
}

func (m *WandModel) decodeTermDict(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	r := bytes.NewReader(data)
	var n int64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return err
	}
	for i := int64(0); i < n; i++ {
		var l uint32
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return err
		}
		tb := make([]byte, l)
		if _, err := io.ReadFull(r, tb); err != nil {
			return err
		}
		var id int32
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return err
		}
		m.overflow[string(tb)] = id
	}
	return nil
}

// ---- wand: block-compressed postings keyed by int32 word-id ----

// encodeWand builds the three WAND members in one pass over the word-id-sorted
// terms (see wandFormatV1):
//
//   - IDX (resident): version byte, nterms, then per term {word-id(i32),
//     rankingOffset(i64)} — the word-id -> ranking byte-offset directory rebuilt into
//     m.termOffsets at load (O(vocabulary) small ints).
//   - RANKING (mmap view, lazy): per term a SELF-CONTAINED Block-Max entry at the
//     offset the IDX records: df(varint), nblk(varint), blockDataBase(varint, the
//     term's absolute offset into BLOCKS), maxTf(byte), minDl(varint), then per block
//     {lastDocGap(varint from the previous block's last ord), blockMaxTf(byte),
//     blockMinDl(varint), blkByteLen(varint)}. decodeTermEntry decodes ONE entry on
//     demand — the resident directory heap is O(query), not O(vocabulary).
//   - BLOCKS (mmap view, block-decoded): per term, per BlockSize-doc block the docID
//     GAPS (varint, from the previous block's last ord) then the block's raw tf bytes.
//
// Works on a build-side model (terms map) and a loaded one (materializeDocIDs/Tfs +
// the resident block-max stats), and is deterministic given identical postings, so a
// build → load → re-Serialize round-trip is byte-identical.
func (m *WandModel) encodeWand() (idx, ranking, blocks []byte) {
	type entry struct {
		id int32
		tp *termPostings
	}
	list := make([]entry, 0, m.NumTerms())
	m.forEachTerm(func(id int32, tp *termPostings) { list = append(list, entry{id, tp}) })
	sort.Slice(list, func(i, j int) bool { return list[i].id < list[j].id }) // deterministic output

	var iw, rw, bw leBuf // index, ranking directory, blocks
	iw.b.WriteByte(wandFormatV1)
	iw.i64(int64(len(list)))
	for _, e := range list {
		tp := e.tp
		if tp.blockLastDoc == nil { // defensive: a build-side term never finalized
			deriveTermStats(tp, m.docLen)
		}
		docs := tp.materializeDocIDs()
		tfs := tp.materializeTfs()
		df := len(docs)
		nblk := (df + BlockSize - 1) / BlockSize

		iw.i32(e.id)
		iw.i64(int64(rw.b.Len())) // ranking offset of this term's self-contained entry

		rw.uvarint(uint64(df))
		rw.uvarint(uint64(nblk))
		rw.uvarint(uint64(bw.b.Len())) // blockDataBase: absolute offset into BLOCKS
		rw.b.WriteByte(tp.maxTf)
		rw.uvarint(uint64(uint32(tp.minDl)))
		var prevLast int64
		for b := 0; b < nblk; b++ {
			lo := b * BlockSize
			hi := lo + BlockSize
			if hi > df {
				hi = df
			}
			blkStart := bw.b.Len()
			prev := prevLast
			for j := lo; j < hi; j++ {
				bw.uvarint(uint64(docs[j] - prev))
				prev = docs[j]
			}
			bw.b.Write(tfs[lo:hi])

			rw.uvarint(uint64(tp.blockLastDoc[b] - prevLast)) // lastDocGap
			rw.b.WriteByte(tp.blockMaxTf[b])
			rw.uvarint(uint64(uint32(tp.blockMinDl[b])))
			rw.uvarint(uint64(bw.b.Len() - blkStart)) // this block's docID/tf byte length
			prevLast = tp.blockLastDoc[b]
		}
	}
	return iw.b.Bytes(), rw.b.Bytes(), bw.b.Bytes()
}

// bindWand binds a LOADED model to its (mmap'd or in-memory) ranking + blocks
// sections and rebuilds the resident word-id -> ranking-offset map from the IDX
// member, WITHOUT expanding any term. Terms decode lazily via decodeTermEntry. It
// sets m.terms = nil (the build-side map is unused on a loaded model).
func (m *WandModel) bindWand(idx, ranking, blocks []byte) error {
	m.terms = nil
	m.termOffsets = map[int32]int64{}
	m.ranking = ranking
	m.blocks = blocks
	if len(idx) == 0 {
		return nil // no terms (empty index)
	}
	if len(idx) < 1+8 {
		return moerr.NewInternalErrorNoCtx("wand: wandidx blob too short")
	}
	if idx[0] != wandFormatV1 {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("wand: unsupported wand format %d", idx[0]))
	}
	nterms := int64(binary.LittleEndian.Uint64(idx[1:9]))
	if nterms < 0 {
		return moerr.NewInternalErrorNoCtx("wand: negative nterms")
	}
	m.termOffsets = make(map[int32]int64, nterms)
	off := 9
	for t := int64(0); t < nterms; t++ {
		if off+12 > len(idx) {
			return moerr.NewInternalErrorNoCtx("wand: wandidx truncated")
		}
		wid := int32(binary.LittleEndian.Uint32(idx[off:]))
		ro := int64(binary.LittleEndian.Uint64(idx[off+4:]))
		off += 12
		if ro < 0 || ro >= int64(len(ranking)) {
			return moerr.NewInternalErrorNoCtx("wand: ranking offset out of range")
		}
		m.termOffsets[wid] = ro
	}
	return nil
}

// decodeTermEntry lazily decodes ONE term's self-contained directory entry at byte
// offset `off` in m.ranking into a transient termPostings whose blockData is a view
// into m.blocks. Callers hold the result for the query's lifetime (WAND cursor,
// forEachTerm), so the entry decodes at most a few times per query and never persists —
// the resident directory heap is O(query), not O(vocabulary). Returns (nil,false) on a
// corrupt/out-of-bounds entry (defense-in-depth on already-checksummed data).
func (m *WandModel) decodeTermEntry(off int64) (*termPostings, bool) {
	r := m.ranking
	if off < 0 || off >= int64(len(r)) {
		return nil, false
	}
	p := int(off)
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
	baseB, ok := uv() // blockDataBase: absolute offset into m.blocks
	if !ok {
		return nil, false
	}
	if p >= len(r) {
		return nil, false
	}
	termMaxTf := r[p]
	p++
	minDlu, ok := uv()
	if !ok {
		return nil, false
	}
	// A df/nblk larger than the blocks section is impossible (each posting is >= 1 byte).
	if dfu > uint64(len(m.blocks)) || nblku > uint64(len(m.blocks)) {
		return nil, false
	}
	df, nblk := int(dfu), int(nblku)
	tp := &termPostings{ndoc: df, maxTf: termMaxTf, minDl: int32(minDlu)}
	if nblk == 0 {
		return tp, true
	}
	tp.blockLastDoc = make([]int64, nblk)
	tp.blockMaxTf = make([]uint8, nblk)
	tp.blockMinDl = make([]int32, nblk)
	tp.blockOff = make([]int32, nblk+1) // byte offsets RELATIVE to this term's blockData
	var prevLast, cb int64
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
		tp.blockMinDl[b] = int32(mdl)
		blkLen, ok := uv()
		if !ok || blkLen > uint64(len(m.blocks)) {
			return nil, false
		}
		tp.blockOff[b] = int32(cb)
		cb += int64(blkLen)
	}
	tp.blockOff[nblk] = int32(cb)
	if int64(baseB)+cb > int64(len(m.blocks)) {
		return nil, false
	}
	tp.blockData = m.blocks[baseB : int64(baseB)+cb]
	return tp, true
}

// ---- pk codec (by types.T) ----

// encodePk serializes a primary-key value to bytes given its type.
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
		// Stored as the canonical TEXT form (like a varlena string), because that
		// is how the SQL-based CDC delivers a uuid pk (extractRowFromVector ->
		// Uuid.String()). The sync build (GetAny) delivers a types.Uuid instead, so
		// stringify it — Uuid.String() is deterministic and scale-free, so the same
		// uuid stores identically regardless of which path produced it. The
		// membership prefilter re-encodes the loaded pk (a types.Uuid) the same way.
		switch x := v.(type) {
		case string:
			return []byte(x), nil
		case types.Uuid:
			return []byte(x.String()), nil
		case []byte:
			return append([]byte(nil), x...), nil
		default:
			return nil, moerr.NewInternalErrorNoCtxf("wand: uuid pk unexpected go type %T", v)
		}
	// Native fixed-width temporal / decimal pks. Delivered natively by the ISCP
	// extractor's ReprNative mode (extractRowFromVector) — NOT as a SQL-display
	// string — so they encode as their exact raw bytes (deterministic, reversible)
	// rather than a lossy round-trip through Datetime.String2()/Decimal.Format().
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
		return nil, moerr.NewInternalErrorNoCtxf("wand: unsupported pk type %d", pkType)
	}
}

// pkFixedWidth returns the fixed byte width encodePk emits for a fixed-width pk
// type and true, or (-1, false) for a variable-length (varlena — varchar/char/
// text/blob/…) type, so callers can distinguish "variable length" (needs a per-pk
// length prefix) from a fixed width. Callers that store many pks (the delete log)
// use it to drop the length prefix for fixed types.
//
// The width comes from types.T.FixedLength() (the canonical source) rather than a
// second hardcoded copy. encodePk handles more than the integer widths (varlena,
// the fixed-width temporal/decimal types, and uuid), but pkFixedWidth deliberately
// fast-paths only the four integer widths: the temporal/decimal types (and uuid,
// which encodePk stores as its variable-looking canonical text form) simply take
// the length-prefixed varlena path below. That is correct — just not maximally
// compact for the fixed-width temporal/decimal pks. Promoting those to fixed-width
// here is a delete-log/docmap on-disk format change and must be done with matching
// encode/decode-symmetry tests, not as an incidental tweak.
func pkFixedWidth(pkType int32) (int, bool) {
	switch t := types.T(pkType); t {
	case types.T_int64, types.T_uint64, types.T_int32, types.T_uint32:
		return t.FixedLength(), true
	default:
		return -1, false // variable-length (varlena) or length-prefixed fixed type
	}
}

// decodePk reverses encodePk, producing the Go value AppendAny expects.
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
		// Stored as canonical text; parse back to types.Uuid — the search output
		// doc_id column is uuid-typed and INNER-JOINed to src.id (apply_indices_
		// fulltext.go), so AppendAny needs a types.Uuid, and a uniform types.Uuid
		// keeps normalizeKey consistent across segments and delete frames.
		u, err := types.ParseUuid(string(b))
		if err != nil {
			return nil, err
		}
		return u, nil
	// Native fixed-width temporal / decimal pks (see encodePk). Reproduce the exact
	// native Go value AppendAny / the membership prefilter expect (the doc_id output
	// column is the same source type, so a uniform native value keeps normalizeKey
	// consistent across segments and delete frames).
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
		return nil, moerr.NewInternalErrorNoCtxf("wand: unsupported pk type %d", pkType)
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
