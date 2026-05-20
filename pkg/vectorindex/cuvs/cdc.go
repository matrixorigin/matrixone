// Copyright 2022 Matrix Origin
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

// Package cuvs carries shared cuvs-specific helpers — currently the
// CDC wire format used by CAGRA and IVF-PQ for tag=1 event chunks.
//
// Lives one directory below pkg/vectorindex so callers can reach
// these helpers without dragging in the entire pkg/vectorindex
// surface, and so future cuvs-only utilities have a natural home.
package cuvs

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// CDC chunk framing.
//
// Every tag=1 chunk on the wire is wrapped in a 32-byte frame so corruption
// (bit flips, truncation, accidental overwrites) is detected at load time
// rather than silently producing wrong replay state.
//
// Layout (all integers little-endian, all fields uint32-aligned, payload
// starts at a 16-aligned offset):
//
//	off  size  field
//	  0  4     magic_start  = 0xCDC51A11
//	  4  4     version      = 1
//	  8  4     payload_len  = N
//	 12  4     reserved     = 0  (room for flags / compression bits)
//	 16  N     records
//	16+N 4     crc32        IEEE over bytes [4 .. 16+N)
//	20+N 4     reserved     = 0
//	24+N 4     reserved     = 0
//	28+N 4     magic_end    = 0xCDC51A11
//
// CRC covers everything between the two magics so a flipped header bit
// (version, payload_len, reserved) is also detected. Both magics are the
// same constant; mismatch on either signals truncation or wrong-row
// corruption.
const (
	cdcChunkMagic    uint32 = 0xCDC51A11
	cdcChunkVersion  uint32 = 1
	cdcHeaderSize           = 16
	cdcFooterSize           = 16
	cdcFrameOverhead        = cdcHeaderSize + cdcFooterSize // 32 bytes
)

// FrameCdcChunk wraps the given record bytes into the on-wire chunk frame
// described above. The returned slice is always exactly len(records)+32
// bytes. Exposed so tests can construct framed chunks directly.
func FrameCdcChunk(records []byte) []byte {
	out := make([]byte, cdcFrameOverhead+len(records))
	binary.LittleEndian.PutUint32(out[0:4], cdcChunkMagic)
	binary.LittleEndian.PutUint32(out[4:8], cdcChunkVersion)
	binary.LittleEndian.PutUint32(out[8:12], uint32(len(records)))
	// out[12:16] reserved, already zero
	copy(out[cdcHeaderSize:cdcHeaderSize+len(records)], records)
	footerOff := cdcHeaderSize + len(records)
	crc := crc32.ChecksumIEEE(out[4:footerOff])
	binary.LittleEndian.PutUint32(out[footerOff:footerOff+4], crc)
	// out[footerOff+4:footerOff+12] reserved, already zero
	binary.LittleEndian.PutUint32(out[footerOff+12:footerOff+16], cdcChunkMagic)
	return out
}

// UnframeCdcChunk validates the frame and returns the record bytes (aliased
// into framed). Returns an error on any framing inconsistency: short input,
// wrong magic, unknown version, length overrun, or CRC mismatch.
func UnframeCdcChunk(framed []byte) ([]byte, error) {
	if len(framed) < cdcFrameOverhead {
		return nil, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: chunk too short (%d bytes < %d)", len(framed), cdcFrameOverhead)
	}
	if got := binary.LittleEndian.Uint32(framed[0:4]); got != cdcChunkMagic {
		return nil, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: bad start magic 0x%08x (want 0x%08x)", got, cdcChunkMagic)
	}
	if v := binary.LittleEndian.Uint32(framed[4:8]); v != cdcChunkVersion {
		return nil, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: unknown version %d (want %d)", v, cdcChunkVersion)
	}
	plen := binary.LittleEndian.Uint32(framed[8:12])
	if uint64(plen)+uint64(cdcFrameOverhead) != uint64(len(framed)) {
		return nil, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: payload_len %d + overhead %d != chunk size %d",
			plen, cdcFrameOverhead, len(framed))
	}
	records := framed[cdcHeaderSize : cdcHeaderSize+plen]
	footerOff := cdcHeaderSize + int(plen)
	gotCrc := binary.LittleEndian.Uint32(framed[footerOff : footerOff+4])
	wantCrc := crc32.ChecksumIEEE(framed[4:footerOff])
	if gotCrc != wantCrc {
		return nil, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: crc32 mismatch got=0x%08x want=0x%08x", gotCrc, wantCrc)
	}
	if got := binary.LittleEndian.Uint32(framed[footerOff+12 : footerOff+16]); got != cdcChunkMagic {
		return nil, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: bad end magic 0x%08x (want 0x%08x)", got, cdcChunkMagic)
	}
	return records, nil
}

// CDC event log helpers shared by CAGRA and IVF-PQ.
//
// CDC writes never touch the model tar (tag=0). They append op-tagged event
// records to a single tag=1 event log. Each chunk holds a temporally-ordered
// batch of records; replay in chunk_id order produces the per-pkid latest
// state automatically — INSERT-then-DELETE collapses to "deleted", and
// DELETE-then-INSERT collapses to "in overflow".
//
// Record layout (variable size by op, fixed size *per* op except Header):
//
//	DELETE: op:byte | pkid:int64                                                  // 9 bytes
//	INSERT: op:byte | pkid:int64 | vec:4*dim | include:K                          // 9 + 4*dim + ibpr
//	HEADER: op:byte | payload_len:uint32 LE | payload:[]byte                      // 5 + N bytes
//
// HEADER carries the index's INCLUDE-column metadata JSON (matching
// cuvscdc.ResolveIncludeColumns output) so the search-side can decode
// subsequent INSERT records when no tag=0 sub-index exists for the index
// slice (small-data-only indexes). Emitted exactly once: as the first
// record of chunk_id=0 by SaveSmallTailAsCdc. Ongoing CagraSync.Save /
// IvfpqSync.Save iterations don't re-emit it.
//
// The HEADER record is self-describing via its own payload_len, so the
// decoder doesn't need ibpr to skip past it.
//
// UPSERT decomposes to DELETE+INSERT at write time (cuvs has no in-place
// mutate; emitting DELETE first preserves last-event-wins semantics if a
// later DELETE arrives for the same pkid).

// CdcOp is the op code stored in the leading byte of each event record.
type CdcOp byte

const (
	CdcOpDelete CdcOp = 0
	CdcOpInsert CdcOp = 1
	CdcOpHeader CdcOp = 2
)

// CdcEventRecord is the decoded form of one tag=1 record.
type CdcEventRecord struct {
	Op      CdcOp
	Pkid    int64
	Vec     []float32 // populated only for CdcOpInsert
	Include []byte    // populated only for CdcOpInsert (and only when includeBytesPerRow > 0)
	Header  []byte    // populated only for CdcOpHeader — the colMetaJSON bytes
}

// EncodeHeaderRecord appends a CdcOpHeader record carrying payload bytes
// (typically the index's INCLUDE-column metadata JSON) to dst. Used by
// SaveSmallTailAsCdc as the first record of chunk_id=0 so the search-side
// can decode subsequent INSERT records when no tag=0 sub-index exists.
//
// Layout: op(1) | payload_len(uint32 LE) | payload(payload_len bytes).
// The self-describing length lets the decoder skip past the header without
// knowing the index's includeBytesPerRow.
func EncodeHeaderRecord(dst, payload []byte) ([]byte, error) {
	if len(payload) > math.MaxUint32 {
		return nil, moerr.NewInternalErrorNoCtxf(
			"EncodeHeaderRecord: payload too large (%d > %d)", len(payload), math.MaxUint32)
	}
	dst = append(dst, byte(CdcOpHeader))
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(payload)))
	dst = append(dst, n[:]...)
	dst = append(dst, payload...)
	return dst, nil
}

// EncodeEventRecord appends one record to dst and returns the new slice.
// vec is required iff op==CdcOpInsert; include is required iff op==CdcOpInsert
// AND includeBytesPerRow > 0. dim must be the index's dimensionality.
//
// CdcOpHeader is not handled here — call EncodeHeaderRecord for headers.
func EncodeEventRecord(
	dst []byte,
	op CdcOp,
	pkid int64,
	vec []float32,
	include []byte,
	dim int,
	includeBytesPerRow int,
) ([]byte, error) {
	switch op {
	case CdcOpDelete:
		if len(vec) != 0 || len(include) != 0 {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: DELETE record must not carry vec/include")
		}
		dst = append(dst, byte(CdcOpDelete))
		var pk [8]byte
		binary.LittleEndian.PutUint64(pk[:], uint64(pkid))
		dst = append(dst, pk[:]...)
		return dst, nil
	case CdcOpInsert:
		if dim <= 0 {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: INSERT requires positive dim, got %d", dim)
		}
		if len(vec) != dim {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: INSERT vec length %d != dim %d", len(vec), dim)
		}
		if includeBytesPerRow > 0 && len(include) != includeBytesPerRow {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: INSERT include length %d != includeBytesPerRow %d",
				len(include), includeBytesPerRow)
		}
		if includeBytesPerRow == 0 && len(include) != 0 {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: includeBytesPerRow=0 but include bytes supplied")
		}
		dst = append(dst, byte(CdcOpInsert))
		var pk [8]byte
		binary.LittleEndian.PutUint64(pk[:], uint64(pkid))
		dst = append(dst, pk[:]...)
		var f [4]byte
		for _, v := range vec {
			binary.LittleEndian.PutUint32(f[:], math.Float32bits(v))
			dst = append(dst, f[:]...)
		}
		if includeBytesPerRow > 0 {
			dst = append(dst, include...)
		}
		return dst, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: unknown op %d", op)
	}
}

// DecodeEventRecord decodes the next record at src[0:]. Returns the record
// and the number of bytes consumed. Returns ok=false when src cannot start a
// valid record (e.g. unknown op byte, or fewer bytes than the record needs)
// — the caller treats this as the end of the stream within the chunk.
//
// Vec and Include in the returned record alias into src; copy if you need to
// retain them past the next call.
func DecodeEventRecord(
	src []byte,
	dim int,
	includeBytesPerRow int,
) (rec CdcEventRecord, n int, ok bool) {
	if len(src) < 1 {
		return rec, 0, false
	}
	op := CdcOp(src[0])
	switch op {
	case CdcOpHeader:
		// HEADER: op(1) | payload_len(4) | payload(payload_len).
		// Self-describing length so we don't need ibpr to skip past it.
		if len(src) < 5 {
			return rec, 0, false
		}
		payloadLen := int(binary.LittleEndian.Uint32(src[1:5]))
		need := 5 + payloadLen
		if len(src) < need {
			return rec, 0, false
		}
		rec.Op = CdcOpHeader
		rec.Header = make([]byte, payloadLen)
		copy(rec.Header, src[5:need])
		return rec, need, true
	case CdcOpDelete:
		if len(src) < 9 {
			return rec, 0, false
		}
		rec.Op = CdcOpDelete
		rec.Pkid = int64(binary.LittleEndian.Uint64(src[1:9]))
		return rec, 9, true
	case CdcOpInsert:
		need := 9 + 4*dim + includeBytesPerRow
		if dim <= 0 || includeBytesPerRow < 0 || len(src) < need {
			return rec, 0, false
		}
		rec.Op = CdcOpInsert
		rec.Pkid = int64(binary.LittleEndian.Uint64(src[1:9]))
		rec.Vec = make([]float32, dim)
		for k := 0; k < dim; k++ {
			rec.Vec[k] = math.Float32frombits(binary.LittleEndian.Uint32(src[9+k*4:]))
		}
		if includeBytesPerRow > 0 {
			rec.Include = make([]byte, includeBytesPerRow)
			copy(rec.Include, src[9+4*dim:need])
		}
		return rec, need, true
	default:
		return rec, 0, false
	}
}

// CdcAppendEventsSql formats one or more INSERT statements that append the
// given encoded event-record bytes as new tag=1 chunks. records is the
// concatenation of records produced by EncodeEventRecord (in temporal order).
// The caller is responsible for not letting a single record straddle a chunk
// boundary; this helper accepts a `recordSizes` slice telling it where the
// record boundaries are so it can pack chunks while respecting them.
//
// Each emitted chunk is wrapped in the CDC frame (see FrameCdcChunk); the
// payload budget per chunk is MaxChunkSize - cdcFrameOverhead so the on-wire
// size stays within MaxChunkSize. Empty records → no SQL.
//
// chunkId starts at startChunkId and increments per emitted chunk.
func CdcAppendEventsSql(
	tblcfg vectorindex.IndexTableConfig,
	indexId string,
	startChunkId int64,
	records []byte,
	recordSizes []int,
) []string {
	if len(records) == 0 || len(recordSizes) == 0 {
		return nil
	}
	maxPayload := vectorindex.MaxChunkSize - cdcFrameOverhead
	sqlPrefix := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", tblcfg.DbName, tblcfg.IndexTable)
	var sqls []string
	var values []string
	chunkId := startChunkId

	off := 0
	i := 0
	for i < len(recordSizes) {
		// Pack as many records as fit in one frame's payload budget without
		// splitting a record across the boundary.
		used := 0
		j := i
		for j < len(recordSizes) && used+recordSizes[j] <= maxPayload {
			used += recordSizes[j]
			j++
		}
		if j == i {
			// A single record is larger than the chunk payload budget —
			// caller bug.
			return nil
		}
		framed := FrameCdcChunk(records[off : off+used])
		values = append(values, fmt.Sprintf("('%s', %d, unhex('%s'), %d)",
			indexId, chunkId, hex.EncodeToString(framed), vectorindex.Tag_CdcEvents))
		chunkId++
		off += used
		i = j
		if len(values) >= 100 {
			sqls = append(sqls, sqlPrefix+strings.Join(values, ", "))
			values = values[:0]
		}
	}
	if len(values) > 0 {
		sqls = append(sqls, sqlPrefix+strings.Join(values, ", "))
	}
	return sqls
}

// CdcLoadEventsSql formats the SELECT that returns every tag=1 chunk for the
// given index_id. No ORDER BY (per repo convention); the caller must sort
// chunks by chunk_id in Go before replay since record ordering across chunks
// matters for last-event-wins semantics.
func CdcLoadEventsSql(tblcfg vectorindex.IndexTableConfig, indexId string) string {
	return fmt.Sprintf(
		"SELECT chunk_id, data FROM `%s`.`%s` WHERE index_id = '%s' AND tag = %d",
		tblcfg.DbName, tblcfg.IndexTable, indexId, vectorindex.Tag_CdcEvents)
}

// EventChunk is one row from CdcLoadEventsSql, wired up so the caller can
// sort by chunk_id before replay.
type EventChunk struct {
	ChunkId int64
	Data    []byte
}

// SortChunks sorts chunks ascending by chunk_id in place.
func SortChunks(chunks []EventChunk) {
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].ChunkId < chunks[j].ChunkId })
}

// ReplayState is the post-replay output: pkids deleted in the main index and
// pkids living in the brute-force overflow with their vec + include bytes.
// The two are mutually exclusive — INSERT-after-DELETE moves the pkid out of
// Deleted, DELETE-after-INSERT moves it out of Overflow.
type ReplayState struct {
	Deleted  []int64
	Overflow []OverflowEntry

	// ColMetaJSON is the payload of a CdcOpHeader record observed during
	// replay, when one was present (small-tail emit path writes it as
	// the first record of chunk_id=0). Empty otherwise. Callers that
	// need the INCLUDE-column layout but have no tag=0 sub-index read
	// this back here.
	ColMetaJSON string
}

// OverflowEntry is one row in the brute-force overflow.
type OverflowEntry struct {
	Pkid    int64
	Vec     []float32
	Include []byte
}

// PeekColMetaJSON returns the colMetaJSON payload of the CdcOpHeader
// record if it is the first record of chunks[0] (the small-tail writer
// guarantees this placement). Returns "" when no header is present —
// either the index has no INCLUDE columns or no small-tail emit ever ran.
//
// Used by the search side when no tag=0 sub-index has loaded: peek the
// header to compute includeBytesPerRow BEFORE calling ReplayEventLog
// (which itself needs ibpr to decode INSERT records). The header record
// is self-describing via its own payload_len, so the decode here doesn't
// depend on ibpr.
//
// Callers must SortChunks first; an empty chunks slice returns "" / nil.
func PeekColMetaJSON(chunks []EventChunk) (string, error) {
	if len(chunks) == 0 {
		return "", nil
	}
	first := chunks[0]
	if first.ChunkId != 0 {
		return "", nil
	}
	data, err := UnframeCdcChunk(first.Data)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", nil
	}
	rec, _, ok := DecodeEventRecord(data, /*dim=*/ 1, /*ibpr=*/ 0)
	if !ok || rec.Op != CdcOpHeader {
		return "", nil
	}
	return string(rec.Header), nil
}

// ReplayEventLog walks the chunks (assumed sorted by chunk_id) and applies
// each record in order, returning the final (deleted, overflow) state. dim
// and includeBytesPerRow describe the INSERT record layout. Replay is O(n)
// in event count.
func ReplayEventLog(
	chunks []EventChunk,
	dim int,
	includeBytesPerRow int,
) (ReplayState, error) {
	if dim <= 0 {
		return ReplayState{}, moerr.NewInternalErrorNoCtxf("ReplayEventLog: invalid dim %d", dim)
	}
	if includeBytesPerRow < 0 {
		return ReplayState{}, moerr.NewInternalErrorNoCtxf("ReplayEventLog: negative includeBytesPerRow %d", includeBytesPerRow)
	}

	deleted := map[int64]struct{}{}
	overflow := map[int64]OverflowEntry{}
	var colMetaJSON string

	for _, ch := range chunks {
		data, err := UnframeCdcChunk(ch.Data)
		if err != nil {
			return ReplayState{}, moerr.NewInternalErrorNoCtxf("ReplayEventLog: chunk_id=%d: %v", ch.ChunkId, err)
		}
		for len(data) > 0 {
			rec, n, ok := DecodeEventRecord(data, dim, includeBytesPerRow)
			if !ok {
				// Frame CRC already validated the payload, so any decode
				// failure here is a record-level bug (encoder/decoder mismatch
				// on dim or includeBytesPerRow).
				return ReplayState{}, moerr.NewInternalErrorNoCtxf(
					"ReplayEventLog: chunk_id=%d: undecodable record at offset %d (dim=%d includeBytesPerRow=%d)",
					ch.ChunkId, len(ch.Data)-cdcFooterSize-len(data), dim, includeBytesPerRow)
			}
			switch rec.Op {
			case CdcOpHeader:
				// First-record-of-chunk_id=0 header carries the INCLUDE-
				// column layout. Capture, don't add to delete/overflow.
				// A later header (shouldn't happen — emitted once at
				// small-tail write time) overwrites the previous capture
				// so the last write wins, matching event-order semantics.
				colMetaJSON = string(rec.Header)
			case CdcOpDelete:
				delete(overflow, rec.Pkid)
				deleted[rec.Pkid] = struct{}{}
			case CdcOpInsert:
				delete(deleted, rec.Pkid)
				overflow[rec.Pkid] = OverflowEntry{
					Pkid:    rec.Pkid,
					Vec:     rec.Vec,
					Include: rec.Include,
				}
			}
			data = data[n:]
		}
	}

	out := ReplayState{
		Deleted:     make([]int64, 0, len(deleted)),
		Overflow:    make([]OverflowEntry, 0, len(overflow)),
		ColMetaJSON: colMetaJSON,
	}
	for p := range deleted {
		out.Deleted = append(out.Deleted, p)
	}
	for _, e := range overflow {
		out.Overflow = append(out.Overflow, e)
	}
	// Stable order so callers (and tests) get deterministic output regardless
	// of map iteration order.
	sort.Slice(out.Deleted, func(i, j int) bool { return out.Deleted[i] < out.Deleted[j] })
	sort.Slice(out.Overflow, func(i, j int) bool { return out.Overflow[i].Pkid < out.Overflow[j].Pkid })
	return out, nil
}

// CdcIncludeBytesPerRow returns the per-row INCLUDE byte size for a given
// column-meta JSON. Format mirrors what gpu_cagra_set_filter_columns
// consumes in cgo/cuvs/cagra_c.h: a JSON array of {"name": ..., "type": N}.
// Returns (0, nil) for empty/whitespace-only JSON or an empty array.
//
// type code → bytes (matches cgo/cuvs/filter.hpp:51-57):
//
//	0 int32   → 4
//	1 int64   → 8
//	2 float32 → 4
//	3 float64 → 8
//	4 uint64  → 8 (varchar hash)
func CdcIncludeBytesPerRow(colMetaJSON string) (int, error) {
	sizes, err := IncludeColSizes(colMetaJSON)
	if err != nil {
		return 0, err
	}
	if len(sizes) == 0 {
		return 0, nil
	}
	total := 0
	for _, s := range sizes {
		total += s
	}
	total += (len(sizes) + 7) / 8 // trailing per-row null mask
	return total, nil
}

// IncludeBinding is one INCLUDE column's resolved binding to a source-table
// column. Built once at CDC writer construction via ResolveIncludeColumns
// and re-used per row to encode the include byte stream.
type IncludeBinding struct {
	Name      string // canonical column name
	Pos       int32  // index into the row []any delivered by ISCP
	TypeCode  int    // colmeta type code: 0=int32, 1=int64, 2=float32, 3=float64, 4=uint64
	SizeBytes int    // element size derived from TypeCode (4 or 8)
}

// Source-table type IDs (mirror pkg/container/types.T constants) that
// INCLUDE columns may use. Kept inline so cuvs_cdc.go stays free of a
// pkg/container/types import — these values are public-API stable.
const (
	srcTypeIDInt32   = 22
	srcTypeIDInt64   = 23
	srcTypeIDUint64  = 28
	srcTypeIDFloat32 = 30
	srcTypeIDFloat64 = 31
)

// ResolveIncludeColumns parses the comma-separated names in
// includedColumns and resolves each against the source table's column
// layout. nameToPos maps a column name to its position in the row []any
// delivered by ISCP; colTypeID returns the source-table type ID
// (matching pkg/container/types.T values) for that position.
//
// Returns:
//   - bindings: per-column resolved (Pos, TypeCode, SizeBytes), declaration order
//   - colMetaJSON: shape that IncludeColSizes / SplitIncludeBytes consume
//     (so writer and sync agree on layout by construction)
//   - includeBytesPerRow: total bytes per row (per-col data + null-mask)
//
// All zero when includedColumns is empty.
func ResolveIncludeColumns(
	includedColumns string,
	nameToPos map[string]int32,
	colTypeID func(pos int32) int32,
) ([]IncludeBinding, string, int, error) {
	trimmed := strings.TrimSpace(includedColumns)
	if trimmed == "" {
		return nil, "", 0, nil
	}
	parts := strings.Split(trimmed, ",")
	bindings := make([]IncludeBinding, 0, len(parts))
	for _, raw := range parts {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		pos, ok := nameToPos[name]
		if !ok {
			return nil, "", 0, moerr.NewInternalErrorNoCtxf(
				"ResolveIncludeColumns: column %q not found in source table", name)
		}
		typeCode, sizeBytes, err := includeTypeFromSrcID(name, colTypeID(pos))
		if err != nil {
			return nil, "", 0, err
		}
		bindings = append(bindings, IncludeBinding{
			Name:      name,
			Pos:       pos,
			TypeCode:  typeCode,
			SizeBytes: sizeBytes,
		})
	}
	if len(bindings) == 0 {
		return nil, "", 0, nil
	}

	// Build colMetaJSON: [{"name":"foo","type":1},...]
	var sb strings.Builder
	sb.WriteByte('[')
	for i, b := range bindings {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"name":"`)
		sb.WriteString(b.Name)
		sb.WriteString(`","type":`)
		sb.WriteString(strconv.Itoa(b.TypeCode))
		sb.WriteByte('}')
	}
	sb.WriteByte(']')
	colMetaJSON := sb.String()

	includeBytesPerRow, err := CdcIncludeBytesPerRow(colMetaJSON)
	if err != nil {
		return nil, "", 0, err
	}
	return bindings, colMetaJSON, includeBytesPerRow, nil
}

func includeTypeFromSrcID(name string, srcTypeID int32) (typeCode, sizeBytes int, err error) {
	switch srcTypeID {
	case srcTypeIDInt32:
		return 0, 4, nil
	case srcTypeIDInt64:
		return 1, 8, nil
	case srcTypeIDFloat32:
		return 2, 4, nil
	case srcTypeIDFloat64:
		return 3, 8, nil
	case srcTypeIDUint64:
		return 4, 8, nil
	default:
		return 0, 0, moerr.NewInternalErrorNoCtxf(
			"ResolveIncludeColumns: column %q has unsupported INCLUDE type id %d "+
				"(supported: int32, int64, uint64, float32, float64)", name, srcTypeID)
	}
}

// EncodeIncludeRow encodes one row's INCLUDE column values into the
// row-major byte layout consumed by IncludeColSizes / SplitIncludeBytes:
//
//	col0_value || col1_value || ... || null_mask
//
// where null_mask is ceil(ncols/8) bytes, LSB-first within each byte
// (bit i = 1 means binding i is NULL). row[binding.Pos] supplies the
// native Go value (int32 / int64 / uint64 / float32 / float64) or nil.
//
// Returns nil when bindings is empty.
func EncodeIncludeRow(bindings []IncludeBinding, row []any, includeBytesPerRow int) ([]byte, error) {
	if len(bindings) == 0 || includeBytesPerRow == 0 {
		return nil, nil
	}
	buf := make([]byte, includeBytesPerRow)
	maskStart := includeBytesPerRow - (len(bindings)+7)/8
	off := 0
	for i, b := range bindings {
		v := row[b.Pos]
		if v == nil {
			buf[maskStart+i/8] |= 1 << (i % 8)
			off += b.SizeBytes
			continue
		}
		switch b.TypeCode {
		case 0: // int32
			x, ok := v.(int32)
			if !ok {
				return nil, moerr.NewInternalErrorNoCtxf(
					"EncodeIncludeRow: column %q expected int32, got %T", b.Name, v)
			}
			binary.LittleEndian.PutUint32(buf[off:], uint32(x))
		case 1: // int64
			x, ok := v.(int64)
			if !ok {
				return nil, moerr.NewInternalErrorNoCtxf(
					"EncodeIncludeRow: column %q expected int64, got %T", b.Name, v)
			}
			binary.LittleEndian.PutUint64(buf[off:], uint64(x))
		case 2: // float32
			x, ok := v.(float32)
			if !ok {
				return nil, moerr.NewInternalErrorNoCtxf(
					"EncodeIncludeRow: column %q expected float32, got %T", b.Name, v)
			}
			binary.LittleEndian.PutUint32(buf[off:], math.Float32bits(x))
		case 3: // float64
			x, ok := v.(float64)
			if !ok {
				return nil, moerr.NewInternalErrorNoCtxf(
					"EncodeIncludeRow: column %q expected float64, got %T", b.Name, v)
			}
			binary.LittleEndian.PutUint64(buf[off:], math.Float64bits(x))
		case 4: // uint64
			x, ok := v.(uint64)
			if !ok {
				return nil, moerr.NewInternalErrorNoCtxf(
					"EncodeIncludeRow: column %q expected uint64, got %T", b.Name, v)
			}
			binary.LittleEndian.PutUint64(buf[off:], x)
		default:
			return nil, moerr.NewInternalErrorNoCtxf(
				"EncodeIncludeRow: column %q unknown type code %d", b.Name, b.TypeCode)
		}
		off += b.SizeBytes
	}
	return buf, nil
}

// IncludeColSizes returns the per-column elem size in bytes for a colMetaJSON.
// Returns nil for empty / no INCLUDE columns. Used both for size accounting
// and for demuxing row-major include bytes into per-column slices.
func IncludeColSizes(colMetaJSON string) ([]int, error) {
	trimmed := strings.TrimSpace(colMetaJSON)
	if trimmed == "" || trimmed == "[]" {
		return nil, nil
	}
	var meta []struct {
		Name string `json:"name"`
		Type int    `json:"type"`
	}
	if err := sonic.Unmarshal([]byte(trimmed), &meta); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("IncludeColSizes: parse colMetaJSON: %v", err)
	}
	if len(meta) == 0 {
		return nil, nil
	}
	sizes := make([]int, len(meta))
	for i, c := range meta {
		switch c.Type {
		case 0, 2: // int32, float32
			sizes[i] = 4
		case 1, 3, 4: // int64, float64, uint64
			sizes[i] = 8
		default:
			return nil, moerr.NewInternalErrorNoCtxf("IncludeColSizes: column %d (%q) unknown type %d",
				i, c.Name, c.Type)
		}
	}
	return sizes, nil
}

// SplitIncludeBytes demuxes the row-major INCLUDE byte stream produced by
// EncodeEventRecord (or a CDC writer) into one row-major byte slice per
// column plus a packed uint32 null bitmap per column in the shape that
// gpu_*_add_filter_chunk consumes (LSB-first; bit i = 1 means row i IS NULL).
//
// includeBytes layout per row: col0_value || col1_value || ... || null_mask
// where null_mask is ceil(ncols/8) bytes, LSB-first within each byte.
//
// Returned colNulls[i] is nil when column i has no nulls in this batch
// (matches the AddFilterChunk convention of skipping the null-mask path).
func SplitIncludeBytes(
	colMetaJSON string,
	includeBytes []byte,
	nrows uint64,
	includeBytesPerRow int,
) ([][]byte, [][]uint32, error) {
	if nrows == 0 || includeBytesPerRow == 0 {
		return nil, nil, nil
	}
	sizes, err := IncludeColSizes(colMetaJSON)
	if err != nil {
		return nil, nil, err
	}
	if len(sizes) == 0 {
		return nil, nil, nil
	}
	expected := uint64(includeBytesPerRow) * nrows
	if uint64(len(includeBytes)) != expected {
		return nil, nil, moerr.NewInternalErrorNoCtxf(
			"SplitIncludeBytes: includeBytes length %d does not match nrows*includeBytesPerRow %d",
			len(includeBytes), expected)
	}

	maskBytes := (len(sizes) + 7) / 8
	dataBytes := includeBytesPerRow - maskBytes

	colData := make([][]byte, len(sizes))
	colNulls := make([][]uint32, len(sizes))
	colHasNulls := make([]bool, len(sizes))
	colOffsets := make([]int, len(sizes))
	off := 0
	for i, s := range sizes {
		colData[i] = make([]byte, int(nrows)*s)
		colOffsets[i] = off
		off += s
	}
	if off != dataBytes {
		return nil, nil, moerr.NewInternalErrorNoCtxf(
			"SplitIncludeBytes: column sizes sum %d != per-row data bytes %d",
			off, dataBytes)
	}

	words := (int(nrows) + 31) / 32
	for i := range sizes {
		colNulls[i] = make([]uint32, words)
	}

	for r := uint64(0); r < nrows; r++ {
		rec := includeBytes[r*uint64(includeBytesPerRow) : (r+1)*uint64(includeBytesPerRow)]
		for i, s := range sizes {
			copy(colData[i][int(r)*s:int(r+1)*s], rec[colOffsets[i]:colOffsets[i]+s])
		}
		mask := rec[dataBytes:]
		for i := range sizes {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			if byteIdx < len(mask) && (mask[byteIdx]>>bitIdx)&1 == 1 {
				colHasNulls[i] = true
				colNulls[i][r/32] |= 1 << (r % 32)
			}
		}
	}
	for i := range sizes {
		if !colHasNulls[i] {
			colNulls[i] = nil
		}
	}
	return colData, colNulls, nil
}

// NextChunkIdSql formats a SELECT that returns the next available chunk_id
// for (index_id, tag). Returns 0 when no rows exist for the given tag.
//
// Caller typically does:
//
//	res, _ := runSql(sqlproc, NextChunkIdSql(...))
//	defer res.Close()
//	next := ParseNextChunkId(res)  // 0 if empty
func NextChunkIdSql(tblcfg vectorindex.IndexTableConfig, indexId string, tag vectorindex.ChunkTag) string {
	// COALESCE(MAX(chunk_id) + 1, 0): no ORDER BY (per repo convention).
	return fmt.Sprintf(
		"SELECT COALESCE(MAX(chunk_id) + 1, 0) FROM `%s`.`%s` WHERE index_id = '%s' AND tag = %d",
		tblcfg.DbName, tblcfg.IndexTable, indexId, tag)
}
