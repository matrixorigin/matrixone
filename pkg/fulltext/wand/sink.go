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
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// The sink side of Phase B: the ISCP consumer's WandSqlWriter accumulates CDC
// rows into a WandCdc, serializes it through the ISCP channel (Encode), and
// RunWand decodes it (DecodeWandCdc) and STREAMS it through a TailBuilder into
// capacity-capped tag=1 CdcTail segments appended to the store (TailFileInsertSqls).
// (BuildTailFrames below is the non-streaming equivalent, kept for tests /
// small in-memory batches.) The blob is BINARY (typed pk via encodePk) — unlike
// the HNSW JSON path, because a retrieval pk is `any` (int64 OR varchar) and a
// JSON round-trip would corrupt a non-int pk.

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

func (c *WandCdc) Insert(pk any, text string) {
	c.Events = append(c.Events, CdcEvent{cdcInsert, pk, text})
}
func (c *WandCdc) Upsert(pk any, text string) {
	c.Events = append(c.Events, CdcEvent{cdcUpsert, pk, text})
}
func (c *WandCdc) Delete(pk any) { c.Events = append(c.Events, CdcEvent{cdcDelete, pk, ""}) }
func (c *WandCdc) Len() int      { return len(c.Events) }

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

// BuildTailFrames turns one in-memory CDC batch into tag=1 CdcTail frames,
// starting at startChunkId, and returns them plus the next free chunk_id.
// INSERT/UPSERT rows are tokenized (via the injected tokenizer — kept out of this
// package so it stays dependency-light and unit-testable) into `capacity`-capped
// delta segments; DELETE rows become one delete frame.
//
// NON-STREAMING: it builds ALL of cdc's inserts in memory before framing, so the
// production sinker uses the streaming TailBuilder instead (bounded to one open
// segment). BuildTailFrames is kept for tests / small batches.
//
// Ordering: the delete frame is emitted FIRST (lowest chunk_id) so a same-batch
// UPDATE (delivered as DELETE old + INSERT new) resolves correctly — the new
// segment sits at a higher chunk_id, and ComputeLiveness kills only segments
// with chunk_id STRICTLY below the delete, so the fresh copy survives while the
// base copy (chunk_id below the delete) is dropped.
//
// A segment frame larger than MaxChunkSize is split across chunk rows at persist
// and reassembled at load (Bug 1) — capacity no longer needs to keep a segment
// within one storage row; it sizes from max_index_capacity.
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
		chunkId += frameChunkCount(len(frame))
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
		chunkId += frameChunkCount(len(frame))
	}
	return frames, chunkId, nil
}

// NextTailChunkIdSql returns a SELECT for the next free tag=1 CdcTail chunk_id
// (COALESCE(MAX+1, 0), scoped to index_id=CdcTailId, tag=Tag_CdcEvents) — the
// monotonic append position the sinker frames at. Mirrors cuVS's NextChunkIdSql.
func NextTailChunkIdSql(cfg TableConfig) string {
	return fmt.Sprintf("SELECT COALESCE(MAX(%s)+1, 0) FROM %s WHERE %s = %s AND %s = %d",
		catalog.FullTextIndex_TblCol_Storage_Chunk_Id, sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullTextIndex_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
}

// FrameChunkCount is the exported form: how many MaxChunkSize storage rows a frame
// of frameLen bytes occupies. The streaming sinker uses it to advance chunk_id past
// each spilled segment without holding the framed bytes.
func FrameChunkCount(frameLen int) int64 { return frameChunkCount(frameLen) }

// frameChunkCount is the number of MaxChunkSize storage rows a frame of this many
// bytes occupies (>= 1). A large segment frame is split across several rows
// because the store's data column is capped at MaxChunkSize (64 KB).
func frameChunkCount(frameLen int) int64 {
	n := int64((frameLen + vectorindex.MaxChunkSize - 1) / vectorindex.MaxChunkSize)
	if n < 1 {
		n = 1
	}
	return n
}

// maxInsertTuples caps VALUES tuples per INSERT (matches HNSW's 2000). Each tuple's
// load_file reads a MaxChunkSize file chunk into memory at execution, so an unbounded
// single INSERT would materialize the whole index at once → OOM / GC pressure. This
// bounds a persist statement to ~maxInsertTuples*MaxChunkSize resident.
const maxInsertTuples = 2000

// FileChunkInsertSqls renders the storage INSERTs that read a FILE directly via
// load_file — no hex/unhex, and (for the streaming sinker) no read-back to memory:
// the frame is ALREADY on disk. It splits [0..dataLen) across MaxChunkSize chunk
// rows from startChunkId under (index_id=id, tag), batching <= maxInsertTuples tuples
// per INSERT. Mirrors HNSW's ToSql. The file MUST exist when the INSERT executes, so
// the caller keeps it until the persist txn commits. A frame larger than MaxChunkSize
// is thus split across consecutive chunk_ids and reassembled at load via CdcFrameLen.
func FileChunkInsertSqls(cfg TableConfig, id string, startChunkId int64, path string, dataLen int, tag int) []string {
	prefix := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES ",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, catalog.FullTextIndex_TblCol_Storage_Chunk_Id,
		catalog.FullTextIndex_TblCol_Storage_Data, catalog.FullTextIndex_TblCol_Storage_Tag)
	var sqls, vals []string
	chunkID := startChunkId
	for off := 0; off < dataLen; off += vectorindex.MaxChunkSize {
		sz := vectorindex.MaxChunkSize
		if off+sz > dataLen {
			sz = dataLen - off
		}
		url := fmt.Sprintf("file://%s?offset=%d&size=%d", path, off, sz)
		vals = append(vals, fmt.Sprintf("(%s, %d, load_file(cast(%s as datalink)), %d)",
			sqlquote.String(id), chunkID, sqlquote.String(url), tag))
		chunkID++
		if len(vals) == maxInsertTuples {
			sqls = append(sqls, prefix+strings.Join(vals, ", "))
			vals = vals[:0]
		}
	}
	if len(vals) > 0 {
		sqls = append(sqls, prefix+strings.Join(vals, ", "))
	}
	return sqls
}

// TailFileInsertSqls is FileChunkInsertSqls for the tag=1 CdcTail (index_id =
// CdcTailId, tag = Tag_CdcEvents) — the streaming sinker's spilled frame files.
func TailFileInsertSqls(cfg TableConfig, startChunkId int64, path string, frameLen int) []string {
	return FileChunkInsertSqls(cfg, vectorindex.CdcTailId, startChunkId, path, frameLen, int(vectorindex.Tag_CdcEvents))
}
