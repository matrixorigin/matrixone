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

package vectorindex

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strings"

	"github.com/bytedance/sonic"
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
		return nil, fmt.Errorf("UnframeCdcChunk: chunk too short (%d bytes < %d)", len(framed), cdcFrameOverhead)
	}
	if got := binary.LittleEndian.Uint32(framed[0:4]); got != cdcChunkMagic {
		return nil, fmt.Errorf("UnframeCdcChunk: bad start magic 0x%08x (want 0x%08x)", got, cdcChunkMagic)
	}
	if v := binary.LittleEndian.Uint32(framed[4:8]); v != cdcChunkVersion {
		return nil, fmt.Errorf("UnframeCdcChunk: unknown version %d (want %d)", v, cdcChunkVersion)
	}
	plen := binary.LittleEndian.Uint32(framed[8:12])
	if uint64(plen)+uint64(cdcFrameOverhead) != uint64(len(framed)) {
		return nil, fmt.Errorf("UnframeCdcChunk: payload_len %d + overhead %d != chunk size %d",
			plen, cdcFrameOverhead, len(framed))
	}
	records := framed[cdcHeaderSize : cdcHeaderSize+plen]
	footerOff := cdcHeaderSize + int(plen)
	gotCrc := binary.LittleEndian.Uint32(framed[footerOff : footerOff+4])
	wantCrc := crc32.ChecksumIEEE(framed[4:footerOff])
	if gotCrc != wantCrc {
		return nil, fmt.Errorf("UnframeCdcChunk: crc32 mismatch got=0x%08x want=0x%08x", gotCrc, wantCrc)
	}
	if got := binary.LittleEndian.Uint32(framed[footerOff+12 : footerOff+16]); got != cdcChunkMagic {
		return nil, fmt.Errorf("UnframeCdcChunk: bad end magic 0x%08x (want 0x%08x)", got, cdcChunkMagic)
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
// Record layout (variable size by op, fixed size *per* op):
//
//	op:byte | pkid:int64 | (if op==CdcOpInsert) vec:4*dim | (if op==CdcOpInsert) include:K
//
// DELETE = 9 bytes; INSERT = 9 + 4*dim + includeBytesPerRow.
//
// UPSERT decomposes to DELETE+INSERT at write time (cuvs has no in-place
// mutate; emitting DELETE first preserves last-event-wins semantics if a
// later DELETE arrives for the same pkid).

// CdcOp is the op code stored in the leading byte of each event record.
type CdcOp byte

const (
	CdcOpDelete CdcOp = 0
	CdcOpInsert CdcOp = 1
)

// CdcEventRecord is the decoded form of one tag=1 record.
type CdcEventRecord struct {
	Op      CdcOp
	Pkid    int64
	Vec     []float32 // populated only for CdcOpInsert
	Include []byte    // populated only for CdcOpInsert (and only when includeBytesPerRow > 0)
}

// EncodeEventRecord appends one record to dst and returns the new slice.
// vec is required iff op==CdcOpInsert; include is required iff op==CdcOpInsert
// AND includeBytesPerRow > 0. dim must be the index's dimensionality.
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
			return nil, fmt.Errorf("EncodeEventRecord: DELETE record must not carry vec/include")
		}
		dst = append(dst, byte(CdcOpDelete))
		var pk [8]byte
		binary.LittleEndian.PutUint64(pk[:], uint64(pkid))
		dst = append(dst, pk[:]...)
		return dst, nil
	case CdcOpInsert:
		if dim <= 0 {
			return nil, fmt.Errorf("EncodeEventRecord: INSERT requires positive dim, got %d", dim)
		}
		if len(vec) != dim {
			return nil, fmt.Errorf("EncodeEventRecord: INSERT vec length %d != dim %d", len(vec), dim)
		}
		if includeBytesPerRow > 0 && len(include) != includeBytesPerRow {
			return nil, fmt.Errorf("EncodeEventRecord: INSERT include length %d != includeBytesPerRow %d",
				len(include), includeBytesPerRow)
		}
		if includeBytesPerRow == 0 && len(include) != 0 {
			return nil, fmt.Errorf("EncodeEventRecord: includeBytesPerRow=0 but include bytes supplied")
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
		return nil, fmt.Errorf("EncodeEventRecord: unknown op %d", op)
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
	if len(src) < 9 {
		return rec, 0, false
	}
	op := CdcOp(src[0])
	switch op {
	case CdcOpDelete:
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
	tblcfg IndexTableConfig,
	indexId string,
	startChunkId int64,
	records []byte,
	recordSizes []int,
) []string {
	if len(records) == 0 || len(recordSizes) == 0 {
		return nil
	}
	maxPayload := MaxChunkSize - cdcFrameOverhead
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
			indexId, chunkId, hex.EncodeToString(framed), Tag_CdcEvents))
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
func CdcLoadEventsSql(tblcfg IndexTableConfig, indexId string) string {
	return fmt.Sprintf(
		"SELECT chunk_id, data FROM `%s`.`%s` WHERE index_id = '%s' AND tag = %d",
		tblcfg.DbName, tblcfg.IndexTable, indexId, Tag_CdcEvents)
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
}

// OverflowEntry is one row in the brute-force overflow.
type OverflowEntry struct {
	Pkid    int64
	Vec     []float32
	Include []byte
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
		return ReplayState{}, fmt.Errorf("ReplayEventLog: invalid dim %d", dim)
	}
	if includeBytesPerRow < 0 {
		return ReplayState{}, fmt.Errorf("ReplayEventLog: negative includeBytesPerRow %d", includeBytesPerRow)
	}

	deleted := map[int64]struct{}{}
	overflow := map[int64]OverflowEntry{}

	for _, ch := range chunks {
		data, err := UnframeCdcChunk(ch.Data)
		if err != nil {
			return ReplayState{}, fmt.Errorf("ReplayEventLog: chunk_id=%d: %w", ch.ChunkId, err)
		}
		for len(data) > 0 {
			rec, n, ok := DecodeEventRecord(data, dim, includeBytesPerRow)
			if !ok {
				// Frame CRC already validated the payload, so any decode
				// failure here is a record-level bug (encoder/decoder mismatch
				// on dim or includeBytesPerRow).
				return ReplayState{}, fmt.Errorf(
					"ReplayEventLog: chunk_id=%d: undecodable record at offset %d (dim=%d includeBytesPerRow=%d)",
					ch.ChunkId, len(ch.Data)-cdcFooterSize-len(data), dim, includeBytesPerRow)
			}
			switch rec.Op {
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
		Deleted:  make([]int64, 0, len(deleted)),
		Overflow: make([]OverflowEntry, 0, len(overflow)),
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
		return nil, fmt.Errorf("IncludeColSizes: parse colMetaJSON: %w", err)
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
			return nil, fmt.Errorf("IncludeColSizes: column %d (%q) unknown type %d",
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
		return nil, nil, fmt.Errorf(
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
		return nil, nil, fmt.Errorf(
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
func NextChunkIdSql(tblcfg IndexTableConfig, indexId string, tag ChunkTag) string {
	// COALESCE(MAX(chunk_id) + 1, 0): no ORDER BY (per repo convention).
	return fmt.Sprintf(
		"SELECT COALESCE(MAX(chunk_id) + 1, 0) FROM `%s`.`%s` WHERE index_id = '%s' AND tag = %d",
		tblcfg.DbName, tblcfg.IndexTable, indexId, tag)
}
