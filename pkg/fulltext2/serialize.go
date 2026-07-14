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
	"hash/crc32"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Tar member names — one archive per segment (same multi-member shape as bm25).
const (
	memberDocmap   = "docmap"   // pkType + N + ord->pk + ord->docLen
	memberTermDict = "termdict" // the vellum FST: term -> ordinal
	memberPostings = "postings" // per-term (sorted): df, docIDs, tfs, positions
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

	fst, postings, err := s.encodeTermsAndPostings()
	if err != nil {
		return nil, err
	}
	if err = writeMember(tw, memberTermDict, fst); err != nil {
		return nil, err
	}
	if err = writeMember(tw, memberPostings, postings); err != nil {
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
	s := &Segment{Id: id}
	var fstBytes, postings []byte
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		b := make([]byte, h.Size)
		if _, err := io.ReadFull(tr, b); err != nil {
			return nil, err
		}
		switch h.Name {
		case memberDocmap:
			if err := s.decodeDocmap(b); err != nil {
				return nil, err
			}
		case memberTermDict:
			fstBytes = b
		case memberPostings:
			postings = b
		}
	}
	if err := s.decodePostings(postings); err != nil {
		return nil, err
	}
	dict, err := loadTermDict(fstBytes)
	if err != nil {
		return nil, err
	}
	s.dict = dict
	s.N = int64(len(s.pks))
	return s, nil
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
	tmp [8]byte
}

func (w *leBuf) u32(v uint32) { binary.LittleEndian.PutUint32(w.tmp[:4], v); w.b.Write(w.tmp[:4]) }
func (w *leBuf) u64(v uint64) { binary.LittleEndian.PutUint64(w.tmp[:8], v); w.b.Write(w.tmp[:8]) }
func (w *leBuf) i32(v int32)  { w.u32(uint32(v)) }
func (w *leBuf) i64(v int64)  { w.u64(uint64(v)) }

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

// encodeTermsAndPostings builds the FST (term -> ordinal) and the postings blob
// in one pass over the sorted terms, so ordinal i in the FST is posting list i in
// the blob.
func (s *Segment) encodeTermsAndPostings() (fst []byte, postings []byte, err error) {
	n := len(s.sortedTerms)
	values := make([]uint64, n)
	var w leBuf
	w.i64(int64(n))
	for i, term := range s.sortedTerms {
		values[i] = uint64(i)
		tp := s.terms[term]
		df := len(tp.docIDs)
		w.u32(uint32(df))
		for _, d := range tp.docIDs {
			w.i64(d)
		}
		w.b.Write(tp.tfs) // df bytes
		for _, pos := range tp.positions {
			w.u32(uint32(len(pos)))
			for _, p := range pos {
				w.i32(p)
			}
		}
	}
	if fst, err = buildTermDictFST(s.sortedTerms, values); err != nil {
		return nil, nil, err
	}
	return fst, w.b.Bytes(), nil
}

// decodePostings reads the postings blob into s.loaded (indexed by ordinal). The
// raw score-UB fields (maxTf / minDocLen / block-max) are NOT stored — they are
// derived from these postings + docLen by the scorer (P1); here we load only the
// exact posting data (docIDs, tfs, positions).
func (s *Segment) decodePostings(data []byte) error {
	r := bytes.NewReader(data)
	var nterms int64
	if err := binary.Read(r, binary.LittleEndian, &nterms); err != nil {
		return err
	}
	s.loaded = make([]*termPostings, nterms)
	for t := int64(0); t < nterms; t++ {
		var df uint32
		if err := binary.Read(r, binary.LittleEndian, &df); err != nil {
			return err
		}
		tp := &termPostings{
			docIDs:    make([]int64, df),
			tfs:       make([]uint8, df),
			positions: make([][]int32, df),
		}
		if err := binary.Read(r, binary.LittleEndian, tp.docIDs); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, tp.tfs); err != nil {
			return err
		}
		for d := uint32(0); d < df; d++ {
			var pc uint32
			if err := binary.Read(r, binary.LittleEndian, &pc); err != nil {
				return err
			}
			pos := make([]int32, pc)
			if err := binary.Read(r, binary.LittleEndian, pos); err != nil {
				return err
			}
			tp.positions[d] = pos
		}
		// Derive the raw score-UB fields (maxTf / minDocLen) from the loaded
		// postings + docLen — used by WAND max-impact bounds. docLen is decoded
		// from the docmap member, which precedes postings in the archive.
		deriveTermStats(tp, s.docLen)
		s.loaded[t] = tp
	}
	return nil
}

// deriveTermStats fills tp.maxTf and tp.minDocLen from its postings (df >= 1).
func deriveTermStats(tp *termPostings, docLen []int32) {
	var maxTf uint8
	minDL := int32(math.MaxInt32)
	for i, ord := range tp.docIDs {
		if tp.tfs[i] > maxTf {
			maxTf = tp.tfs[i]
		}
		if dl := docLen[ord]; dl < minDL {
			minDL = dl
		}
	}
	tp.maxTf = maxTf
	tp.minDocLen = minDL
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
