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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// Deserialize parses a tar archive produced by Serialize.
func Deserialize(id string, data []byte) (*WandModel, error) {
	tr := tar.NewReader(bytes.NewReader(data))
	members := map[string][]byte{}
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
		members[h.Name] = b
	}
	m := NewWandModel(id, 0)
	if err := m.decodeDocmap(members[memberDocmap]); err != nil {
		return nil, err
	}
	if err := m.decodeTermDict(members[memberTermDict]); err != nil {
		return nil, err
	}
	if err := m.decodeWand(members[memberWand]); err != nil {
		return nil, err
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
	for id := range m.terms {
		ids = append(ids, id)
	}
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

func (m *WandModel) decodeWand(data []byte) error {
	r := bytes.NewReader(data)
	var nterms int64
	if err := binary.Read(r, binary.LittleEndian, &nterms); err != nil {
		return err
	}
	for t := int64(0); t < nterms; t++ {
		var id int32
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return err
		}
		var df uint32
		if err := binary.Read(r, binary.LittleEndian, &df); err != nil {
			return err
		}
		tp := &termPostings{docIDs: make([]int64, df), tfs: make([]uint8, df)}
		if err := binary.Read(r, binary.LittleEndian, tp.docIDs); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, tp.tfs); err != nil {
			return err
		}
		m.terms[id] = tp
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
