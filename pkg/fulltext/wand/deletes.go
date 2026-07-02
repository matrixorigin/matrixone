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
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// The retrieval index's tag=1 "delete log" (see fulltext_wand.md, Phase B). Each
// CDC DELETE/UPSERT emits one DeleteRecord (just the pk). A batch of records is
// appended as one framed tag=1 chunk alongside the tag=1 delta segments in the
// same ft_index store; the frame's chunk_id (its append position in the single
// CdcTail log) is the delete's order — there is NO stored order field. At search
// the frames are decoded and folded, in chunk_id order, into a
// pk -> maxDeleteChunkId map fed to ComputeLiveness.
//
// This is the structural analog of cuVS's deleted-set: delete-then-reinsert /
// UPDATE resolve correctly across immutable segments because a delete only kills
// segments with chunk_id < its frame's chunk_id (see ComputeLiveness). The codec
// is self-contained (no dependency on the GPU-coupled cuVS package), matching
// serialize.go's binary+crc32 style.

// DeleteRecord is one tombstone: a pk deleted by a CDC batch. Its order is the
// containing frame's chunk_id (assigned at load), not a stored field.
type DeleteRecord struct {
	Pk any
}

const deleteLogMagic uint32 = 0x57440100 // 'W' 'D' 01 00

// EncodeDeleteLog serializes delete records into one self-describing,
// CRC32-checked chunk: magic | pkType | count | [pkLen:uint32 pk]* | crc.
// pkType is the source PK's types.T (records encode pk via encodePk). No order
// field — the frame's chunk_id is the order.
func EncodeDeleteLog(pkType int32, recs []DeleteRecord) ([]byte, error) {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, deleteLogMagic)
	_ = binary.Write(&b, binary.LittleEndian, pkType)
	_ = binary.Write(&b, binary.LittleEndian, int64(len(recs)))
	for _, r := range recs {
		pkb, err := encodePk(pkType, r.Pk)
		if err != nil {
			return nil, err
		}
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(pkb)))
		b.Write(pkb)
	}
	sum := crc32.ChecksumIEEE(b.Bytes())
	_ = binary.Write(&b, binary.LittleEndian, sum)
	return b.Bytes(), nil
}

// DecodeDeleteLog reverses EncodeDeleteLog, validating magic + CRC.
func DecodeDeleteLog(buf []byte) ([]DeleteRecord, error) {
	if len(buf) < 4+4+8+4 {
		return nil, moerr.NewInternalErrorNoCtx("wand delete log: truncated")
	}
	body := buf[:len(buf)-4]
	wantCRC := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if crc32.ChecksumIEEE(body) != wantCRC {
		return nil, moerr.NewInternalErrorNoCtx("wand delete log: checksum mismatch")
	}
	r := bytes.NewReader(body)
	var magic uint32
	_ = binary.Read(r, binary.LittleEndian, &magic)
	if magic != deleteLogMagic {
		return nil, moerr.NewInternalErrorNoCtx("wand delete log: bad magic")
	}
	var pkType int32
	if err := binary.Read(r, binary.LittleEndian, &pkType); err != nil {
		return nil, err
	}
	var n int64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, moerr.NewInternalErrorNoCtx("wand delete log: bad count")
	}
	out := make([]DeleteRecord, 0, n)
	for i := int64(0); i < n; i++ {
		var l uint32
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return nil, err
		}
		pkb := make([]byte, l)
		if _, err := r.Read(pkb); err != nil && l > 0 {
			return nil, err
		}
		pk, err := decodePk(pkType, pkb)
		if err != nil {
			return nil, err
		}
		out = append(out, DeleteRecord{Pk: pk})
	}
	return out, nil
}

// FoldDeleteFrame folds one decoded delete frame — all records share the frame's
// chunk_id — into the running pk -> maxDeleteChunkId map ComputeLiveness
// consumes. Keyed by normalizeKey(pk); the max chunk_id wins, so folding frames
// in any order is idempotent and a redelivered DELETE (a later frame at a higher
// chunk_id) only raises the bound. Pass the accumulator across frames (nil to
// start); returns the same map (allocated on first non-empty frame, nil if no
// records were ever folded).
func FoldDeleteFrame(m map[any]int64, recs []DeleteRecord, chunkId int64) map[any]int64 {
	if len(recs) == 0 {
		return m
	}
	if m == nil {
		m = make(map[any]int64, len(recs))
	}
	for _, r := range recs {
		k := normalizeKey(r.Pk)
		if cur, ok := m[k]; !ok || chunkId > cur {
			m[k] = chunkId
		}
	}
	return m
}
