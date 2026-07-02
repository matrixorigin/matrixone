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
	"encoding/hex"
	"fmt"
	"hash/crc32"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// The sink side of Phase B: the ISCP consumer's WandSqlWriter accumulates CDC
// rows into a WandCdc, serializes it through the ISCP channel (Encode), and
// RunWand decodes it (DecodeWandCdc) and turns it into tag=1 CdcTail frames
// (BuildTailFrames) appended to the store (FrameInsertSql). The blob is BINARY
// (typed pk via encodePk) — unlike the HNSW JSON path, because a retrieval pk is
// `any` (int64 OR varchar) and a JSON round-trip would corrupt a non-int pk.

type wandCdcOp byte

const (
	cdcInsert wandCdcOp = 'I'
	cdcUpsert wandCdcOp = 'U'
	cdcDelete wandCdcOp = 'D'
)

// CdcEvent is one source-row mutation: an INSERT/UPSERT carries the row's text
// (tokenized at build), a DELETE carries only the pk.
type CdcEvent struct {
	Op   wandCdcOp
	Pk   any
	Text string
}

// WandCdc is the per-flush CDC batch the sinker accumulates and ships as one
// channel blob.
type WandCdc struct {
	PkType int32
	Events []CdcEvent
}

func NewWandCdc(pkType int32) *WandCdc { return &WandCdc{PkType: pkType} }

func (c *WandCdc) Insert(pk any, text string) { c.Events = append(c.Events, CdcEvent{cdcInsert, pk, text}) }
func (c *WandCdc) Upsert(pk any, text string) { c.Events = append(c.Events, CdcEvent{cdcUpsert, pk, text}) }
func (c *WandCdc) Delete(pk any)              { c.Events = append(c.Events, CdcEvent{cdcDelete, pk, ""}) }
func (c *WandCdc) Len() int                   { return len(c.Events) }

const wandCdcMagic uint32 = 0x57440200 // 'W' 'D' 02 00

// Encode serializes the batch: magic | pkType | count |
// [op:1 pkLen:u32 pk textLen:u32 text]* | crc32. Self-describing + CRC-checked.
func (c *WandCdc) Encode() ([]byte, error) {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, wandCdcMagic)
	_ = binary.Write(&b, binary.LittleEndian, c.PkType)
	_ = binary.Write(&b, binary.LittleEndian, int64(len(c.Events)))
	for _, e := range c.Events {
		pkb, err := encodePk(c.PkType, e.Pk)
		if err != nil {
			return nil, err
		}
		b.WriteByte(byte(e.Op))
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(pkb)))
		b.Write(pkb)
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(e.Text)))
		b.WriteString(e.Text)
	}
	sum := crc32.ChecksumIEEE(b.Bytes())
	_ = binary.Write(&b, binary.LittleEndian, sum)
	return b.Bytes(), nil
}

// DecodeWandCdc reverses Encode, validating magic + CRC.
func DecodeWandCdc(buf []byte) (*WandCdc, error) {
	if len(buf) < 4+4+8+4 {
		return nil, moerr.NewInternalErrorNoCtx("wand cdc: truncated")
	}
	body := buf[:len(buf)-4]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, moerr.NewInternalErrorNoCtx("wand cdc: checksum mismatch")
	}
	r := bytes.NewReader(body)
	var magic uint32
	_ = binary.Read(r, binary.LittleEndian, &magic)
	if magic != wandCdcMagic {
		return nil, moerr.NewInternalErrorNoCtx("wand cdc: bad magic")
	}
	c := &WandCdc{}
	if err := binary.Read(r, binary.LittleEndian, &c.PkType); err != nil {
		return nil, err
	}
	var n int64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, moerr.NewInternalErrorNoCtx("wand cdc: bad count")
	}
	c.Events = make([]CdcEvent, 0, n)
	for i := int64(0); i < n; i++ {
		op, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		pk, err := readLenBytes(r, c.PkType)
		if err != nil {
			return nil, err
		}
		text, err := readLenString(r)
		if err != nil {
			return nil, err
		}
		c.Events = append(c.Events, CdcEvent{Op: wandCdcOp(op), Pk: pk, Text: text})
	}
	return c, nil
}

func readLenBytes(r *bytes.Reader, pkType int32) (any, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return nil, err
	}
	pkb := make([]byte, l)
	if _, err := r.Read(pkb); err != nil && l > 0 {
		return nil, err
	}
	return decodePk(pkType, pkb)
}

func readLenString(r *bytes.Reader) (string, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return "", err
	}
	sb := make([]byte, l)
	if _, err := r.Read(sb); err != nil && l > 0 {
		return "", err
	}
	return string(sb), nil
}

// BuildTailFrames turns one CDC batch into tag=1 CdcTail frames, starting at
// startChunkId, and returns them plus the next free chunk_id. INSERT/UPSERT rows
// are tokenized (via the injected tokenizer — kept out of this package so it
// stays dependency-light and unit-testable) into one delta segment (split at
// `capacity` docs); DELETE rows become one delete frame.
//
// Ordering: the delete frame is emitted FIRST (lowest chunk_id) so a same-batch
// UPDATE (delivered as DELETE old + INSERT new) resolves correctly — the new
// segment sits at a higher chunk_id, and ComputeLiveness kills only segments
// with chunk_id STRICTLY below the delete, so the fresh copy survives while the
// base copy (chunk_id below the delete) is dropped.
//
// NOTE: `capacity` must keep a serialized segment within vectorindex.MaxChunkSize
// (one frame = one chunk row; the load path does not reassemble a frame across
// rows). The sinker sizes it from max_index_capacity.
func BuildTailFrames(cdc *WandCdc, capacity int64, startChunkId int64, tokenize func(string) []string) ([]TailFrame, int64, error) {
	b := NewBuilder(fmt.Sprintf("cdctail-%d", startChunkId), cdc.PkType)
	var deletes []DeleteRecord
	for _, e := range cdc.Events {
		switch e.Op {
		case cdcInsert, cdcUpsert:
			for _, w := range tokenize(e.Text) {
				if err := b.Add(w, e.Pk); err != nil {
					return nil, 0, err
				}
			}
		case cdcDelete:
			deletes = append(deletes, DeleteRecord{Pk: e.Pk})
		}
	}

	var frames []TailFrame
	chunkId := startChunkId
	if len(deletes) > 0 {
		frame, err := FrameDeletes(cdc.PkType, deletes)
		if err != nil {
			return nil, 0, err
		}
		frames = append(frames, TailFrame{ChunkId: chunkId, Data: frame})
		chunkId++
	}
	for _, seg := range b.FinishSegments(capacity) {
		if seg.N == 0 {
			seg.Free()
			continue
		}
		frame, err := FrameSegment(seg)
		seg.Free()
		if err != nil {
			return nil, 0, err
		}
		frames = append(frames, TailFrame{ChunkId: chunkId, Data: frame})
		chunkId++
	}
	return frames, chunkId, nil
}

// FrameInsertSql renders one tag=1 CdcTail frame as an INSERT into the store
// (index_id = CdcTailId, tag = Tag_CdcEvents), mirroring ToInsertSqls' row form.
func FrameInsertSql(cfg TableConfig, chunkId int64, framed []byte) string {
	return fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES (%s, %d, unhex('%s'), %d)",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, catalog.FullTextIndex_TblCol_Storage_Chunk_Id,
		catalog.FullTextIndex_TblCol_Storage_Data, catalog.FullTextIndex_TblCol_Storage_Tag,
		sqlquote.String(vectorindex.CdcTailId), chunkId, hex.EncodeToString(framed), int(vectorindex.Tag_CdcEvents))
}
