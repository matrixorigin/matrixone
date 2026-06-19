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
// CDC DELETE/UPSERT emits one DeleteRecord (pk + the batch LSN at which it was
// deleted). The log is appended as tag=1 chunks alongside the tag=1 delta
// segments in the same ft_index store; at search it is decoded into a
// pk -> maxDeleteLSN map and fed to ComputeLiveness.
//
// This is the structural analog of cuVS's deleted-set, but carrying the LSN so
// delete-then-reinsert / UPDATE resolve correctly across immutable segments
// (a delete only kills segments with LSN < its deleteLSN — see ComputeLiveness).
// The codec is self-contained (no dependency on the GPU-coupled cuVS package),
// matching serialize.go's binary+crc32 style.

// DeleteRecord is one tombstone: pk deleted as of batch LSN.
type DeleteRecord struct {
	Pk  any
	LSN int64
}

const deleteLogMagic uint32 = 0x57440100 // 'W' 'D' 01 00

// EncodeDeleteLog serializes delete records into one self-describing,
// CRC32-checked chunk: magic | pkType | count | [lsn:int64 pkLen:uint32 pk]* | crc.
// pkType is the source PK's types.T (records encode pk via encodePk).
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
		_ = binary.Write(&b, binary.LittleEndian, r.LSN)
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
		var lsn int64
		if err := binary.Read(r, binary.LittleEndian, &lsn); err != nil {
			return nil, err
		}
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
		out = append(out, DeleteRecord{Pk: pk, LSN: lsn})
	}
	return out, nil
}

// DeleteMap folds decoded delete records into the pk -> maxDeleteLSN map that
// ComputeLiveness consumes. Keyed by normalizeKey(pk); the max LSN wins so the
// fold is order-independent and idempotent (a redelivered DELETE is a no-op).
func DeleteMap(recs []DeleteRecord) map[any]int64 {
	if len(recs) == 0 {
		return nil
	}
	m := make(map[any]int64, len(recs))
	for _, r := range recs {
		k := normalizeKey(r.Pk)
		if cur, ok := m[k]; !ok || r.LSN > cur {
			m[k] = r.LSN
		}
	}
	return m
}
