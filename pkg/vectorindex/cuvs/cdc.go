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
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// CDC chunk framing.
//
// Every tag=1 chunk on the wire is wrapped in a frame so corruption
// (bit flips, truncation, accidental overwrites) is detected at load time
// rather than silently producing wrong replay state.
//
// Layout (all integers little-endian, all fields uint32-aligned, header
// section starts at a 28-aligned offset):
//
//	off       size  field
//	  0       4     magic_start  = 0xCDC51A11
//	  4       4     version      = 1
//	  8       4     n_inserts    (count of CdcOpInsert records in this chunk)
//	 12       4     n_deletes    (count of CdcOpDelete records in this chunk)
//	 16       4     n_upserts    (count of CdcOpUpsert records in this chunk)
//	 20       4     payload_len  = N (size of records section)
//	 24       4     header_len   = H (size of header section, 0 when no INCLUDE)
//	 28       H     header bytes (typically colMetaJSON; aligned, no padding)
//	 28+H     N     records
//	 28+H+N   4     crc32        IEEE over bytes [4 .. 28+H+N)
//	 32+H+N   4     reserved     = 0
//	 36+H+N   4     reserved     = 0
//	 40+H+N   4     magic_end    = 0xCDC51A11
//
// n_inserts, n_deletes, and n_upserts let idxcron compute net change
// without walking every record (DecodeEventRecord), and let logging /
// observability surface the insert-vs-upsert breakdown — useful given
// MO UPSERTs that "are mostly first-time inserts." The CRC covers all
// three counters, so a flipped count byte is detected.
//
// The header section carries the INCLUDE-column metadata JSON when present
// so each chunk is self-describing: search-side decode of the records can
// recover includeBytesPerRow without needing a tag=0 sub-index (the
// small-data-only path), and any chunk read in isolation knows its own
// layout. Empty header (H=0) is the common case for indexes with no
// INCLUDE columns; the frame degrades to a 44-byte overhead.
//
// CRC covers the full header+records section so a flipped header bit or
// drifted header_len is detected. Both magics are the same constant;
// mismatch on either signals truncation or wrong-row corruption.
const (
	cdcChunkMagic    uint32 = 0xCDC51A11
	cdcChunkVersion  uint32 = 1
	cdcHeaderSize           = 28
	cdcFooterSize           = 16
	cdcFrameOverhead        = cdcHeaderSize + cdcFooterSize // 44 bytes, ex. header section
)

// FrameCdcChunk wraps the given record bytes (plus an optional header,
// typically colMetaJSON) into the on-wire chunk frame described above.
// nInserts / nDeletes / nUpserts are the per-op record counts contained
// in `records`, written into the frame header so downstream consumers
// (idxcron Updatable, replay) can read counts without walking every
// record.
//
// The returned slice is exactly cdcFrameOverhead + len(header) + len(records)
// bytes. Exposed so tests can construct framed chunks directly.
//
// Pass header=nil when the chunk has no INCLUDE-column metadata.
func FrameCdcChunk(records, header []byte, nInserts, nDeletes, nUpserts uint32) []byte {
	hlen := len(header)
	rlen := len(records)
	out := make([]byte, cdcFrameOverhead+hlen+rlen)
	binary.LittleEndian.PutUint32(out[0:4], cdcChunkMagic)
	binary.LittleEndian.PutUint32(out[4:8], cdcChunkVersion)
	binary.LittleEndian.PutUint32(out[8:12], nInserts)
	binary.LittleEndian.PutUint32(out[12:16], nDeletes)
	binary.LittleEndian.PutUint32(out[16:20], nUpserts)
	binary.LittleEndian.PutUint32(out[20:24], uint32(rlen))
	binary.LittleEndian.PutUint32(out[24:28], uint32(hlen))
	copy(out[cdcHeaderSize:cdcHeaderSize+hlen], header)
	copy(out[cdcHeaderSize+hlen:cdcHeaderSize+hlen+rlen], records)
	footerOff := cdcHeaderSize + hlen + rlen
	crc := crc32.ChecksumIEEE(out[4:footerOff])
	binary.LittleEndian.PutUint32(out[footerOff:footerOff+4], crc)
	// out[footerOff+4:footerOff+12] reserved, already zero
	binary.LittleEndian.PutUint32(out[footerOff+12:footerOff+16], cdcChunkMagic)
	return out
}

// UnframeCdcChunk validates the frame and returns the record bytes plus
// the header bytes (both aliased into framed) and the per-op counts
// (inserts, deletes, upserts) recorded at frame time. Returns an error
// on any framing inconsistency: short input, wrong magic, unknown
// version, length overrun, or CRC mismatch.
//
// header is nil when the chunk has no INCLUDE-column metadata (H=0).
func UnframeCdcChunk(framed []byte) (records, header []byte, nInserts, nDeletes, nUpserts uint32, err error) {
	if len(framed) < cdcFrameOverhead {
		return nil, nil, 0, 0, 0, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: chunk too short (%d bytes < %d)", len(framed), cdcFrameOverhead)
	}
	if got := binary.LittleEndian.Uint32(framed[0:4]); got != cdcChunkMagic {
		return nil, nil, 0, 0, 0, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: bad start magic 0x%08x (want 0x%08x)", got, cdcChunkMagic)
	}
	if v := binary.LittleEndian.Uint32(framed[4:8]); v != cdcChunkVersion {
		return nil, nil, 0, 0, 0, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: unknown version %d (want %d)", v, cdcChunkVersion)
	}
	nInserts = binary.LittleEndian.Uint32(framed[8:12])
	nDeletes = binary.LittleEndian.Uint32(framed[12:16])
	nUpserts = binary.LittleEndian.Uint32(framed[16:20])
	plen := binary.LittleEndian.Uint32(framed[20:24])
	hlen := binary.LittleEndian.Uint32(framed[24:28])
	if uint64(plen)+uint64(hlen)+uint64(cdcFrameOverhead) != uint64(len(framed)) {
		return nil, nil, 0, 0, 0, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: payload_len %d + header_len %d + overhead %d != chunk size %d",
			plen, hlen, cdcFrameOverhead, len(framed))
	}
	footerOff := cdcHeaderSize + int(hlen) + int(plen)
	gotCrc := binary.LittleEndian.Uint32(framed[footerOff : footerOff+4])
	wantCrc := crc32.ChecksumIEEE(framed[4:footerOff])
	if gotCrc != wantCrc {
		return nil, nil, 0, 0, 0, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: crc32 mismatch got=0x%08x want=0x%08x", gotCrc, wantCrc)
	}
	if got := binary.LittleEndian.Uint32(framed[footerOff+12 : footerOff+16]); got != cdcChunkMagic {
		return nil, nil, 0, 0, 0, moerr.NewInternalErrorNoCtxf("UnframeCdcChunk: bad end magic 0x%08x (want 0x%08x)", got, cdcChunkMagic)
	}
	if hlen > 0 {
		header = framed[cdcHeaderSize : cdcHeaderSize+int(hlen)]
	}
	records = framed[cdcHeaderSize+int(hlen) : cdcHeaderSize+int(hlen)+int(plen)]
	return records, header, nInserts, nDeletes, nUpserts, nil
}

// CdcFrameLen returns the total on-wire byte length of the CDC chunk frame whose
// leading bytes are `prefix` (at least cdcHeaderSize bytes — the frame header is
// self-describing). It lets a consumer that STORES one frame split across several
// fixed-size storage chunks reassemble it: read the first stored chunk's header,
// get the total length, then read that many bytes across the following chunks.
// cuVS's own tail packs small records so a frame is always <= one chunk; the WAND
// retrieval index stores indivisible segment blobs that can exceed a chunk, hence
// this helper. Validates the start magic; does not require the full frame.
func CdcFrameLen(prefix []byte) (int, error) {
	if len(prefix) < cdcHeaderSize {
		return 0, moerr.NewInternalErrorNoCtxf("CdcFrameLen: prefix too short (%d < %d)", len(prefix), cdcHeaderSize)
	}
	if got := binary.LittleEndian.Uint32(prefix[0:4]); got != cdcChunkMagic {
		return 0, moerr.NewInternalErrorNoCtxf("CdcFrameLen: bad start magic 0x%08x (want 0x%08x)", got, cdcChunkMagic)
	}
	plen := binary.LittleEndian.Uint32(prefix[20:24])
	hlen := binary.LittleEndian.Uint32(prefix[24:28])
	return cdcFrameOverhead + int(hlen) + int(plen), nil
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
//	DELETE: op:byte | pkid:int64                                                  // 9 bytes
//	INSERT: op:byte | pkid:int64 | vec:4*dim | include:K                          // 9 + 4*dim + ibpr
//	UPSERT: op:byte | pkid:int64 | vec:4*dim | include:K                          // same payload as INSERT
//
// INCLUDE-column metadata (colMetaJSON) is NOT a record — it lives in the
// chunk frame's header section (see FrameCdcChunk). Records are pure
// event payloads.
//
// UPSERT and INSERT have identical payload shapes but distinct op codes
// and distinct replay semantics:
//
//	INSERT — guaranteed-new row → goes only into the brute-force overflow.
//	UPSERT — may replace an existing main-index entry → acts as
//	         DELETE + INSERT at the replay-state level: marks pkid in the
//	         deleted set (so any old main-index entry is filtered at
//	         search time) AND writes the new vec/include into the
//	         brute-force overflow.
//
// They're kept distinct because MO UPSERT is ambiguous: it may be a real
// row replacement, a retry of an already-applied event, or a replay from
// the start after stream corruption. Only true INSERT is guaranteed to
// be a brand-new row, so INSERT can skip the deleted-set entry while
// UPSERT cannot. The idxcron frame counter (n_inserts in FrameCdcChunk)
// only counts CdcOpInsert so the cron gate sees a strict lower bound on
// growth. DELETE for an unknown pkid is silent — overflow-map delete is
// a Go no-op and the deleted set is idempotent.

// CdcOp is the op code stored in the leading byte of each event record.
type CdcOp byte

const (
	CdcOpDelete CdcOp = 0
	CdcOpInsert CdcOp = 1
	// CdcOpUpsert has the same payload as CdcOpInsert (op|pkid|vec|include)
	// but is encoded distinctly so downstream consumers can ignore UPSERTs
	// when computing reliable new-row counts (see package comment).
	CdcOpUpsert CdcOp = 2
)

// CdcEventRecord is the decoded form of one tag=1 record. Vec holds the raw
// little-endian vector bytes in the index's native base element type (4 bytes
// per element for vecf32, 2 for a vecf16) — the codec is element-type-agnostic;
// the GPU layer reinterprets these bytes to []float32 / []cuvs.Float16.
type CdcEventRecord struct {
	Op      CdcOp
	Pkid    int64
	Vec     []byte // populated only for CdcOpInsert (vecBytesPerRow bytes)
	Include []byte // populated only for CdcOpInsert (and only when includeBytesPerRow > 0)
}

// EncodeEventRecord appends one record to dst and returns the new slice.
// vec is required iff op==CdcOpInsert; include is required iff op==CdcOpInsert
// AND includeBytesPerRow > 0. vec carries the row's vector as raw native
// base-type bytes; vecBytesPerRow is its expected length (dim * element size,
// e.g. 4*dim for f32, 2*dim for f16).
func EncodeEventRecord(
	dst []byte,
	op CdcOp,
	pkid int64,
	vec []byte,
	include []byte,
	vecBytesPerRow int,
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
	case CdcOpInsert, CdcOpUpsert:
		// UPSERT shares INSERT's payload layout — only the op byte differs.
		opName := "INSERT"
		if op == CdcOpUpsert {
			opName = "UPSERT"
		}
		if vecBytesPerRow <= 0 {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: %s requires positive vecBytesPerRow, got %d", opName, vecBytesPerRow)
		}
		if len(vec) != vecBytesPerRow {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: %s vec length %d != vecBytesPerRow %d", opName, len(vec), vecBytesPerRow)
		}
		if includeBytesPerRow > 0 && len(include) != includeBytesPerRow {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: %s include length %d != includeBytesPerRow %d",
				opName, len(include), includeBytesPerRow)
		}
		if includeBytesPerRow == 0 && len(include) != 0 {
			return nil, moerr.NewInternalErrorNoCtxf("EncodeEventRecord: includeBytesPerRow=0 but include bytes supplied")
		}
		dst = append(dst, byte(op))
		var pk [8]byte
		binary.LittleEndian.PutUint64(pk[:], uint64(pkid))
		dst = append(dst, pk[:]...)
		// vec body: raw native base-type bytes, copied verbatim. For f32 this is
		// byte-identical to the legacy math.Float32bits + PutUint32 form on
		// little-endian targets, so existing f32 CDC streams are unchanged.
		dst = append(dst, vec...)
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
// vecBytesPerRow is the INSERT-record vector byte length (dim * element size).
func DecodeEventRecord(
	src []byte,
	vecBytesPerRow int,
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
	case CdcOpInsert, CdcOpUpsert:
		need := 9 + vecBytesPerRow + includeBytesPerRow
		if vecBytesPerRow <= 0 || includeBytesPerRow < 0 || len(src) < need {
			return rec, 0, false
		}
		rec.Op = op
		rec.Pkid = int64(binary.LittleEndian.Uint64(src[1:9]))
		// vec body: raw native base-type bytes, copied verbatim.
		rec.Vec = make([]byte, vecBytesPerRow)
		copy(rec.Vec, src[9:9+vecBytesPerRow])
		if includeBytesPerRow > 0 {
			rec.Include = make([]byte, includeBytesPerRow)
			copy(rec.Include, src[9+vecBytesPerRow:need])
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
//
// colMetaJSON is the INCLUDE-column metadata (cuvscdc.ResolveIncludeColumns
// output, e.g. `[{"name":"a","type":1},...]`); when non-empty it is
// embedded in EVERY emitted chunk's frame header so the search-side can
// decode the chunk's records (and recover ibpr) without depending on a
// tag=0 sub-index. Pass "" when the index has no INCLUDE columns.
func CdcAppendEventsSql(
	tblcfg vectorindex.IndexTableConfig,
	indexId string,
	startChunkId int64,
	records []byte,
	recordSizes []int,
	colMetaJSON string,
) ([]string, error) {
	if len(records) == 0 || len(recordSizes) == 0 {
		return nil, nil
	}
	var headerBytes []byte
	if colMetaJSON != "" {
		headerBytes = []byte(colMetaJSON)
	}
	// Per-chunk byte budget for RECORDS, after accounting for the
	// frame's fixed overhead and the embedded colMetaJSON header.
	maxPayload := vectorindex.MaxChunkSize - cdcFrameOverhead - len(headerBytes)
	if maxPayload <= 0 {
		// colMetaJSON alone consumes the whole chunk budget — caller bug.
		return nil, moerr.NewInternalErrorNoCtxf(
			"cdc: chunk header (%d bytes) leaves no room for records in a %d-byte chunk (overhead %d)",
			len(headerBytes), vectorindex.MaxChunkSize, cdcFrameOverhead)
	}
	sqlPrefix := fmt.Sprintf("INSERT INTO %s VALUES ", sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.IndexTable))
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
			// A single record is larger than the whole per-chunk payload budget.
			// Error out (rather than silently returning nil) so the CREATE/REINDEX
			// fails loudly instead of dropping rows that would never enter the
			// index. Practically unreachable: it needs a vector dimension above
			// ~16K (record = 9 + 4*dim + includeBytes must exceed maxPayload).
			return nil, moerr.NewInternalErrorNoCtxf(
				"cdc: record %d (%d bytes) exceeds the per-chunk payload budget of %d bytes",
				i, recordSizes[i], maxPayload)
		}
		// Count per-op records in this chunk by peeking at byte 0 of each
		// record (the op code) using recordSizes to step through.
		var nInserts, nDeletes, nUpserts uint32
		recOff := off
		for k := i; k < j; k++ {
			switch CdcOp(records[recOff]) {
			case CdcOpInsert:
				nInserts++
			case CdcOpDelete:
				nDeletes++
			case CdcOpUpsert:
				nUpserts++
			}
			recOff += recordSizes[k]
		}
		framed := FrameCdcChunk(records[off:off+used], headerBytes, nInserts, nDeletes, nUpserts)
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
	return sqls, nil
}

// CdcLoadEventsSql formats the SELECT that returns every tag=1 chunk for the
// given index_id. No ORDER BY (per repo convention); the caller must sort
// chunks by chunk_id in Go before replay since record ordering across chunks
// matters for last-event-wins semantics.
func CdcLoadEventsSql(tblcfg vectorindex.IndexTableConfig, indexId string) string {
	return fmt.Sprintf(
		"SELECT chunk_id, data FROM %s WHERE index_id = %s AND tag = %d",
		sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.IndexTable), sqlquote.String(indexId), vectorindex.Tag_CdcEvents)
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

	// ColMetaJSON is the INCLUDE-column metadata carried in the chunk
	// frame's header section (see FrameCdcChunk / PeekColMetaJSON), not a
	// record — every chunk embeds it, so it is read from the first chunk
	// during replay. Empty when the index has no INCLUDE columns. Callers
	// that need the INCLUDE-column layout but have no tag=0 sub-index read
	// this back here.
	ColMetaJSON string
}

// OverflowEntry is one row in the brute-force overflow. Vec holds the raw
// native base-type bytes (no f32 widening for vecf16); the GPU layer
// reinterprets them to the base element type B.
type OverflowEntry struct {
	Pkid    int64
	Vec     []byte
	Include []byte
}

// PeekColMetaJSON returns the colMetaJSON embedded in the first chunk's
// frame header section. Every chunk carries the header (writer-side
// invariant of CdcAppendEventsSql), so chunks[0] is always sufficient
// — no need to sort or scan. Returns "" when the header section is
// empty (the index has no INCLUDE columns) or chunks is empty.
//
// Used by the search side when no tag=0 sub-index has loaded: peek the
// header to compute includeBytesPerRow BEFORE calling ReplayEventLog
// (which itself needs ibpr to decode INSERT records).
func PeekColMetaJSON(chunks []EventChunk) (string, error) {
	if len(chunks) == 0 {
		return "", nil
	}
	_, header, _, _, _, err := UnframeCdcChunk(chunks[0].Data)
	if err != nil {
		return "", err
	}
	return string(header), nil
}

// ReplayEventLog walks the chunks (assumed sorted by chunk_id) and applies
// each record in order, returning the final (deleted, overflow) state.
// vecBytesPerRow (dim * element size) and includeBytesPerRow describe the
// INSERT record layout. Replay is O(n) in event count.
func ReplayEventLog(
	chunks []EventChunk,
	vecBytesPerRow int,
	includeBytesPerRow int,
) (ReplayState, error) {
	if vecBytesPerRow <= 0 {
		return ReplayState{}, moerr.NewInternalErrorNoCtxf("ReplayEventLog: invalid vecBytesPerRow %d", vecBytesPerRow)
	}
	if includeBytesPerRow < 0 {
		return ReplayState{}, moerr.NewInternalErrorNoCtxf("ReplayEventLog: negative includeBytesPerRow %d", includeBytesPerRow)
	}

	deleted := map[int64]struct{}{}
	overflow := map[int64]OverflowEntry{}
	var colMetaJSON string

	for _, ch := range chunks {
		data, header, _, _, _, err := UnframeCdcChunk(ch.Data)
		if err != nil {
			return ReplayState{}, moerr.NewInternalErrorNoCtxf("ReplayEventLog: chunk_id=%d: %v", ch.ChunkId, err)
		}
		// Every chunk frame carries the INCLUDE-column layout (when
		// the index has INCLUDE columns). Last-write-wins; in practice
		// every chunk's header is identical so the final value matches
		// the index's true layout.
		if len(header) > 0 {
			colMetaJSON = string(header)
		}
		for len(data) > 0 {
			rec, n, ok := DecodeEventRecord(data, vecBytesPerRow, includeBytesPerRow)
			if !ok {
				// Frame CRC already validated the payload, so any decode
				// failure here is a record-level bug (encoder/decoder mismatch
				// on dim or includeBytesPerRow).
				return ReplayState{}, moerr.NewInternalErrorNoCtxf(
					"ReplayEventLog: chunk_id=%d: undecodable record at offset %d (vecBytesPerRow=%d includeBytesPerRow=%d)",
					ch.ChunkId, len(ch.Data)-cdcFooterSize-len(data), vecBytesPerRow, includeBytesPerRow)
			}
			switch rec.Op {
			case CdcOpDelete:
				// DELETE: drop from overflow if present (silent no-op when
				// absent — Go map delete is idempotent), mark in the
				// deleted set so any existing main-index entry is filtered
				// out at search time.
				delete(overflow, rec.Pkid)
				deleted[rec.Pkid] = struct{}{}
			case CdcOpInsert:
				// INSERT: guaranteed-new row → goes ONLY into the
				// brute-force overflow. We deliberately do NOT clear
				// the pkid from the deleted set: any prior DELETE was
				// against the pre-rebuild main-index entry for this
				// pkid, and that filter must persist (un-filtering it
				// would re-expose the stale main-index version,
				// producing a duplicate result alongside the new
				// overflow version). The deleted set governs the
				// main cuvs index; the overflow map governs the
				// brute-force index; they're independent.
				overflow[rec.Pkid] = OverflowEntry{
					Pkid:    rec.Pkid,
					Vec:     rec.Vec,
					Include: rec.Include,
				}
			case CdcOpUpsert:
				// UPSERT is semantically DELETE + INSERT at the replay-
				// state level: mark in the deleted set (so any pre-rebuild
				// main-index entry for this pkid is filtered out at search
				// time) AND write the new version into the brute-force
				// overflow. Idempotent — replaying the same UPSERT or a
				// stream-corruption re-emission just re-writes the same
				// (pkid, vec, include).
				deleted[rec.Pkid] = struct{}{}
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

	// Build colMetaJSON via the shared producer so column names
	// containing `"` or `\` escape correctly and every producer
	// emits the identical wire shape.
	entries := make([]ColMetaEntry, len(bindings))
	for i, b := range bindings {
		entries[i] = ColMetaEntry{Name: b.Name, Type: b.TypeCode}
	}
	colMetaJSON, err := MarshalColMetaJSON(entries)
	if err != nil {
		return nil, "", 0, err
	}

	includeBytesPerRow, err := CdcIncludeBytesPerRow(colMetaJSON)
	if err != nil {
		return nil, "", 0, err
	}
	return bindings, colMetaJSON, includeBytesPerRow, nil
}

// ColMetaEntry is the canonical on-wire shape of one INCLUDE-column
// descriptor in colMetaJSON. The JSON shape is
// `[{"name":"foo","type":1},...]` — the C++ FilterStore + Go decoders
// expect exactly that. Producers Marshal a []ColMetaEntry via
// MarshalColMetaJSON.
type ColMetaEntry struct {
	Name string `json:"name"`
	Type int    `json:"type"`
}

// MarshalColMetaJSON encodes the canonical colMetaJSON for a list of
// INCLUDE-column descriptors. Single producer used by both the iscp
// writer (via ResolveIncludeColumns) and the build-time table
// function (via colMetaJSONFromCols) so the wire shape is identical
// and column names containing `"` or `\` escape correctly.
//
// Returns "" when entries is empty so callers can skip embedding a
// header without a special case.
func MarshalColMetaJSON(entries []ColMetaEntry) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	out, err := json.Marshal(entries)
	if err != nil {
		return "", err
	}
	return string(out), nil
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
		"SELECT COALESCE(MAX(chunk_id) + 1, 0) FROM %s WHERE index_id = %s AND tag = %d",
		sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.IndexTable), sqlquote.String(indexId), tag)
}
