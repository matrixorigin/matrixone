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
	"hash/crc32"
	"io"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Tar member names (cuVS-style multi-member archive).
const (
	memberDocmap   = "docmap"   // pkType + ord -> pk value
	memberTermDict = "termdict" // out-of-dict term -> overflow word-id
	memberWand     = "wand"     // postings keyed by int32 word-id
)

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
	if err := writeMember(tw, memberWand, m.encodeWand()); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize parses a tar archive produced by Serialize, streaming from r so a
// multi-GB index is never fully materialized on the Go heap: the small docmap /
// termdict members are buffered, but the large `wand` postings are read directly
// into off-heap (C-allocated) buffers. r is typically the temp file the storage
// chunks were streamed into.
func Deserialize(id string, r io.Reader) (*WandModel, error) {
	m := NewWandModel(id, 0)
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch h.Name {
		case memberDocmap:
			b := make([]byte, h.Size)
			if _, err := io.ReadFull(tr, b); err != nil {
				return nil, err
			}
			if err := m.decodeDocmap(b); err != nil {
				return nil, err
			}
		case memberTermDict:
			b := make([]byte, h.Size)
			if _, err := io.ReadFull(tr, b); err != nil {
				return nil, err
			}
			if err := m.decodeTermDict(b); err != nil {
				return nil, err
			}
		case memberWand:
			if err := m.decodeWand(tr); err != nil { // streams postings into C buffers
				return nil, err
			}
		}
	}
	m.N = int64(len(m.pks))
	m.finalizeScoring() // derive AvgDocLen + per-term max BM25 factor
	return m, nil
}

func writeMember(tw *tar.Writer, name string, data []byte) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// ---- docmap: pkType + ord -> pk ----

func (m *WandModel) encodeDocmap() ([]byte, error) {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, m.PkType)
	_ = binary.Write(&b, binary.LittleEndian, int64(len(m.pks)))
	for _, pk := range m.pks {
		raw, err := encodePk(m.PkType, pk)
		if err != nil {
			return nil, err
		}
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(raw)))
		b.Write(raw)
	}
	// per-doc length (ord-aligned with pks), for BM25.
	_ = binary.Write(&b, binary.LittleEndian, m.docLen)
	return b.Bytes(), nil
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
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, int64(len(m.overflow)))
	terms := make([]string, 0, len(m.overflow))
	for t := range m.overflow {
		terms = append(terms, t)
	}
	sort.Strings(terms) // deterministic output
	for _, term := range terms {
		tb := []byte(term)
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(tb)))
		b.Write(tb)
		_ = binary.Write(&b, binary.LittleEndian, m.overflow[term])
	}
	return b.Bytes()
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

// ---- wand: postings keyed by int32 word-id ----

func (m *WandModel) encodeWand() []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, int64(len(m.terms)))
	ids := make([]int32, 0, len(m.terms))
	var totalP int64
	for id := range m.terms {
		ids = append(ids, id)
		totalP += int64(len(m.terms[id].docIDs))
	}
	_ = binary.Write(&b, binary.LittleEndian, totalP)               // total postings, for one off-heap alloc on load
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] }) // deterministic output
	for _, id := range ids {
		tp := m.terms[id]
		_ = binary.Write(&b, binary.LittleEndian, id)
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(tp.docIDs)))
		_ = binary.Write(&b, binary.LittleEndian, tp.docIDs)
		b.Write(tp.tfs)
	}
	return b.Bytes()
}

func (m *WandModel) decodeWand(r io.Reader) error {
	var nterms, totalP int64
	if err := binary.Read(r, binary.LittleEndian, &nterms); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &totalP); err != nil {
		return err
	}

	// Allocate all postings OFF the Go heap (C allocator) in two contiguous
	// buffers; each term's docIDs/tfs is a slice into them. This keeps a
	// multi-GB index off the Go heap (no GC scan) and out of the query mpool;
	// freed by Free() on cache eviction. OOM surfaces as an error here.
	if totalP > 0 {
		alloc := malloc.NewCAllocator()
		ob, odec, err := alloc.Allocate(uint64(totalP)*uint64(util.UnsafeSizeOf[int64]()), malloc.NoClear)
		if err != nil {
			return err
		}
		m.deallocators = append(m.deallocators, odec)
		m.bigOrds = util.UnsafeSliceCastToLength[int64](ob, int(totalP))

		tb, tdec, err := alloc.Allocate(uint64(totalP), malloc.NoClear)
		if err != nil {
			return err
		}
		m.deallocators = append(m.deallocators, tdec)
		m.bigTfs = util.UnsafeSliceCastToLength[uint8](tb, int(totalP))
	}

	var cur int64
	for t := int64(0); t < nterms; t++ {
		var id int32
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return err
		}
		var df uint32
		if err := binary.Read(r, binary.LittleEndian, &df); err != nil {
			return err
		}
		if cur+int64(df) > totalP {
			return moerr.NewInternalErrorNoCtx("wand: postings overflow totalP")
		}
		ords := m.bigOrds[cur : cur+int64(df)]
		tfs := m.bigTfs[cur : cur+int64(df)]
		if err := binary.Read(r, binary.LittleEndian, ords); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, tfs); err != nil {
			return err
		}
		m.terms[id] = &termPostings{docIDs: ords, tfs: tfs}
		cur += int64(df)
	}
	return nil
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
	default:
		return nil, moerr.NewInternalErrorNoCtxf("wand: unsupported pk type %d", pkType)
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
