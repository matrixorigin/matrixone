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
	"hash/crc32"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// DeleteRecord is one tombstone: a pk deleted by a CDC batch. Its order is the
// containing frame's chunk_id (assigned at load), not a stored field. Mirrors
// bm25's DeleteRecord.
type DeleteRecord struct {
	Pk any
}

const deleteLogMagic uint32 = 0x46540100 // 'F' 'T' 01 00 — fulltext2 delete log

// pkFixedWidth reports the fixed byte width of an integer pk type (so the delete
// log can skip the per-pk length prefix); varlena/other types are length-prefixed.
func pkFixedWidth(pkType int32) (int, bool) {
	switch t := types.T(pkType); t {
	case types.T_int64, types.T_uint64, types.T_int32, types.T_uint32:
		return t.FixedLength(), true
	default:
		return -1, false
	}
}

// EncodeDeleteLog serializes delete records into one self-describing, CRC32-checked
// chunk: magic | pkType | count | pks | crc. A fixed-width int pk is stored bare
// (width implied by pkType); a varlena pk is length-prefixed. Mirrors bm25's
// EncodeDeleteLog.
func EncodeDeleteLog(pkType int32, recs []DeleteRecord) ([]byte, error) {
	var w leBuf
	w.u32(deleteLogMagic)
	w.i32(pkType)
	w.i64(int64(len(recs)))
	width, fixed := pkFixedWidth(pkType)
	for _, r := range recs {
		pkb, err := encodePk(pkType, r.Pk)
		if err != nil {
			return nil, err
		}
		if fixed {
			if len(pkb) != width {
				return nil, moerr.NewInternalErrorNoCtxf("fulltext2 delete log: pk width %d != %d for fixed type %d", len(pkb), width, pkType)
			}
		} else {
			w.u32(uint32(len(pkb)))
		}
		w.b.Write(pkb)
	}
	sum := crc32.ChecksumIEEE(w.b.Bytes())
	w.u32(sum)
	return w.b.Bytes(), nil
}

// DecodeDeleteLog reverses EncodeDeleteLog, validating magic + CRC.
func DecodeDeleteLog(buf []byte) ([]DeleteRecord, error) {
	if len(buf) < 4+4+8+4 {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 delete log: truncated")
	}
	body := buf[:len(buf)-4]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 delete log: checksum mismatch")
	}
	if binary.LittleEndian.Uint32(body[0:4]) != deleteLogMagic {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 delete log: bad magic")
	}
	pkType := int32(binary.LittleEndian.Uint32(body[4:8]))
	n := int64(binary.LittleEndian.Uint64(body[8:16]))
	if n < 0 {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2 delete log: bad count")
	}
	width, fixed := pkFixedWidth(pkType)
	pos := 16
	out := make([]DeleteRecord, 0, n)
	for i := int64(0); i < n; i++ {
		l := width
		if !fixed {
			if pos+4 > len(body) {
				return nil, moerr.NewInternalErrorNoCtx("fulltext2 delete log: truncated pk length")
			}
			l = int(binary.LittleEndian.Uint32(body[pos:]))
			pos += 4
		}
		if l < 0 || pos+l > len(body) {
			return nil, moerr.NewInternalErrorNoCtx("fulltext2 delete log: truncated pk")
		}
		pk, err := decodePk(pkType, body[pos:pos+l])
		if err != nil {
			return nil, err
		}
		pos += l
		out = append(out, DeleteRecord{Pk: pk})
	}
	return out, nil
}

// foldDeleteFrame folds one decoded delete frame (all records share the frame's
// chunk_id) into the pk-key -> max-delete-chunk_id map that Index liveness
// consumes: a copy with Recency < the delete chunk_id is shadowed. Keyed by
// keyOf(pk) (matching resolve()); the max chunk_id wins, so re-delivered deletes
// only raise the bound. Mirrors bm25's FoldDeleteFrame (string-keyed here).
func foldDeleteFrame(m map[string]int64, recs []DeleteRecord, chunkId int64) map[string]int64 {
	if len(recs) == 0 {
		return m
	}
	if m == nil {
		m = make(map[string]int64)
	}
	for _, r := range recs {
		k := keyOf(r.Pk)
		if cur, ok := m[k]; !ok || chunkId > cur {
			m[k] = chunkId
		}
	}
	return m
}
