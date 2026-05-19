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
	"math"
	"regexp"
	"strings"
	"testing"
)

func testTblcfg() IndexTableConfig {
	return IndexTableConfig{
		DbName:     "db",
		IndexTable: "__cuvs_index",
	}
}

// extractUnhexBlobs pulls every unhex('....') literal out of a SQL string in
// document order. Caller decodes them back to []byte.
var unhexRe = regexp.MustCompile(`unhex\('([0-9a-fA-F]*)'\)`)

func extractUnhexBlobs(t *testing.T, sql string) [][]byte {
	t.Helper()
	matches := unhexRe.FindAllStringSubmatch(sql, -1)
	out := make([][]byte, 0, len(matches))
	for _, m := range matches {
		raw, err := hex.DecodeString(m[1])
		if err != nil {
			t.Fatalf("invalid hex in SQL: %v", err)
		}
		out = append(out, raw)
	}
	return out
}

// encodeBatch is a test helper: encode a slice of (op, pkid, vec, include)
// triples into a single record-stream buffer plus a recordSizes index, in
// the shape CdcAppendEventsSql expects.
func encodeBatch(
	t *testing.T,
	dim int,
	includeBytesPerRow int,
	ops []CdcOp,
	pkids []int64,
	vecs [][]float32,
	includes [][]byte,
) ([]byte, []int) {
	t.Helper()
	if len(ops) != len(pkids) {
		t.Fatalf("encodeBatch: ops/pkids length mismatch")
	}
	var buf []byte
	sizes := make([]int, 0, len(ops))
	insertIdx := 0
	for i, op := range ops {
		var v []float32
		var inc []byte
		if op == CdcOpInsert {
			if insertIdx >= len(vecs) {
				t.Fatalf("encodeBatch: ran out of INSERT vecs at i=%d", i)
			}
			v = vecs[insertIdx]
			if includeBytesPerRow > 0 {
				if insertIdx >= len(includes) {
					t.Fatalf("encodeBatch: ran out of INSERT includes at i=%d", i)
				}
				inc = includes[insertIdx]
			}
			insertIdx++
		}
		before := len(buf)
		out, err := EncodeEventRecord(buf, op, pkids[i], v, inc, dim, includeBytesPerRow)
		if err != nil {
			t.Fatalf("EncodeEventRecord(%v, pkid=%d): %v", op, pkids[i], err)
		}
		buf = out
		sizes = append(sizes, len(buf)-before)
	}
	return buf, sizes
}

// TestEncodeDecodeEventRecord_Delete: round-trip a DELETE record.
func TestEncodeDecodeEventRecord_Delete(t *testing.T) {
	for _, pkid := range []int64{1, -7, math.MaxInt64, math.MinInt64, 0} {
		buf, err := EncodeEventRecord(nil, CdcOpDelete, pkid, nil, nil, 4, 0)
		if err != nil {
			t.Fatalf("encode pkid=%d: %v", pkid, err)
		}
		if len(buf) != 9 {
			t.Fatalf("DELETE record should be 9 bytes, got %d", len(buf))
		}
		rec, n, ok := DecodeEventRecord(buf, 4, 0)
		if !ok || n != 9 {
			t.Fatalf("decode failed: ok=%v n=%d", ok, n)
		}
		if rec.Op != CdcOpDelete || rec.Pkid != pkid {
			t.Fatalf("got op=%v pkid=%d, want DELETE %d", rec.Op, rec.Pkid, pkid)
		}
	}
}

// TestEncodeDecodeEventRecord_Insert: round-trip INSERT record bits.
func TestEncodeDecodeEventRecord_Insert(t *testing.T) {
	dim := 3
	pkid := int64(42)
	vec := []float32{1.5, -2.25, math.MaxFloat32}
	buf, err := EncodeEventRecord(nil, CdcOpInsert, pkid, vec, nil, dim, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := 9 + 4*dim
	if len(buf) != want {
		t.Fatalf("INSERT record len %d, want %d", len(buf), want)
	}
	rec, n, ok := DecodeEventRecord(buf, dim, 0)
	if !ok || n != want {
		t.Fatalf("decode: ok=%v n=%d", ok, n)
	}
	if rec.Op != CdcOpInsert || rec.Pkid != pkid {
		t.Fatalf("op/pkid mismatch")
	}
	for i, v := range vec {
		if math.Float32bits(rec.Vec[i]) != math.Float32bits(v) {
			t.Fatalf("vec[%d]: got %v want %v", i, rec.Vec[i], v)
		}
	}
	if len(rec.Include) != 0 {
		t.Fatalf("expected empty Include, got %d bytes", len(rec.Include))
	}
}

// TestEncodeDecodeEventRecord_InsertWithInclude: INSERT carries include bytes.
func TestEncodeDecodeEventRecord_InsertWithInclude(t *testing.T) {
	dim := 2
	includeBytesPerRow := 4 + 8 + 1 // int32 + int64 + 1 mask byte
	include := make([]byte, includeBytesPerRow)
	binary.LittleEndian.PutUint32(include[0:4], 0xdeadbeef)
	binary.LittleEndian.PutUint64(include[4:12], 0x1122334455667788)
	include[12] = 0x02
	buf, err := EncodeEventRecord(nil, CdcOpInsert, 1, []float32{0.1, 0.2}, include, dim, includeBytesPerRow)
	if err != nil {
		t.Fatal(err)
	}
	want := 9 + 4*dim + includeBytesPerRow
	if len(buf) != want {
		t.Fatalf("len %d, want %d", len(buf), want)
	}
	rec, n, ok := DecodeEventRecord(buf, dim, includeBytesPerRow)
	if !ok || n != want {
		t.Fatalf("decode: ok=%v n=%d", ok, n)
	}
	if len(rec.Include) != includeBytesPerRow {
		t.Fatalf("include len %d, want %d", len(rec.Include), includeBytesPerRow)
	}
	for i, b := range include {
		if rec.Include[i] != b {
			t.Fatalf("include[%d]: got %02x want %02x", i, rec.Include[i], b)
		}
	}
}

// TestEncodeEventRecord_Rejects: encoder rejects malformed inputs.
func TestEncodeEventRecord_Rejects(t *testing.T) {
	// DELETE with vec.
	if _, err := EncodeEventRecord(nil, CdcOpDelete, 1, []float32{1}, nil, 1, 0); err == nil {
		t.Fatal("expected error on DELETE with vec")
	}
	// INSERT with wrong dim.
	if _, err := EncodeEventRecord(nil, CdcOpInsert, 1, []float32{1, 2}, nil, 4, 0); err == nil {
		t.Fatal("expected error on dim mismatch")
	}
	// INSERT with include but includeBytesPerRow=0.
	if _, err := EncodeEventRecord(nil, CdcOpInsert, 1, []float32{1}, []byte{0xff}, 1, 0); err == nil {
		t.Fatal("expected error on extraneous include bytes")
	}
	// Unknown op.
	if _, err := EncodeEventRecord(nil, CdcOp(99), 1, nil, nil, 1, 0); err == nil {
		t.Fatal("expected error on unknown op")
	}
}

// TestDecodeEventRecord_StopsAtPad: decoder returns ok=false on a zero-pad
// (op byte 0 is DELETE which decodes happily to pkid=0 — instead we test the
// no-bytes-left case and an unknown op byte at the boundary).
func TestDecodeEventRecord_StopsAtPad(t *testing.T) {
	// Empty buffer: decoder reports not-ok.
	if _, _, ok := DecodeEventRecord(nil, 4, 0); ok {
		t.Fatal("decoder should not accept empty input")
	}
	// 7 bytes: not enough for any record.
	if _, _, ok := DecodeEventRecord(make([]byte, 7), 4, 0); ok {
		t.Fatal("decoder should not accept 7 bytes")
	}
	// Bogus op byte.
	bogus := []byte{42, 0, 0, 0, 0, 0, 0, 0, 0}
	if _, _, ok := DecodeEventRecord(bogus, 4, 0); ok {
		t.Fatal("decoder should reject unknown op byte")
	}
	// INSERT op but truncated payload.
	short := []byte{byte(CdcOpInsert), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} // missing vec bytes
	if _, _, ok := DecodeEventRecord(short, 4, 0); ok {
		t.Fatal("decoder should reject truncated INSERT")
	}
}

// TestCdcAppendEventsSql_Empty asserts no SQL for an empty batch.
func TestCdcAppendEventsSql_Empty(t *testing.T) {
	if got := CdcAppendEventsSql(testTblcfg(), "idx-1", 0, nil, nil); len(got) != 0 {
		t.Fatalf("expected no SQL for empty batch, got %d", len(got))
	}
}

// TestCdcAppendEventsSql_DeleteOnly: a DELETE-only batch encodes 9-byte
// records and lands in one chunk.
func TestCdcAppendEventsSql_DeleteOnly(t *testing.T) {
	pkids := []int64{1, -7, math.MaxInt64, math.MinInt64}
	ops := []CdcOp{CdcOpDelete, CdcOpDelete, CdcOpDelete, CdcOpDelete}
	buf, sizes := encodeBatch(t, 4, 0, ops, pkids, nil, nil)

	sqls := CdcAppendEventsSql(testTblcfg(), "idx-1", 0, buf, sizes)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 SQL, got %d", len(sqls))
	}
	if !strings.HasPrefix(sqls[0], "INSERT INTO `db`.`__cuvs_index` VALUES ") {
		t.Fatalf("unexpected prefix")
	}
	if !strings.Contains(sqls[0], fmt.Sprintf(", %d)", Tag_CdcEvents)) {
		t.Fatalf("missing tag=%d trailer", Tag_CdcEvents)
	}
	blobs := extractUnhexBlobs(t, sqls[0])
	if len(blobs) != 1 || len(blobs[0]) != cdcFrameOverhead+9*len(pkids) {
		t.Fatalf("unexpected blob shape: %d blobs, first %d bytes", len(blobs), len(blobs[0]))
	}
	// Round-trip via the loader path.
	chunks := []EventChunk{{ChunkId: 0, Data: blobs[0]}}
	state, err := ReplayEventLog(chunks, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Deleted) != 4 || len(state.Overflow) != 0 {
		t.Fatalf("unexpected replay: deleted=%v overflow=%v", state.Deleted, state.Overflow)
	}
}

// TestCdcAppendEventsSql_InsertOnly: pure-INSERT batch round-trips through
// replay to overflow only, deleted=∅.
func TestCdcAppendEventsSql_InsertOnly(t *testing.T) {
	dim := 3
	pkids := []int64{10, 20, 30}
	vecs := [][]float32{
		{1, 2, 3},
		{-1.5, 0, 0.5},
		{0.1, 0.2, 0.3},
	}
	ops := []CdcOp{CdcOpInsert, CdcOpInsert, CdcOpInsert}
	buf, sizes := encodeBatch(t, dim, 0, ops, pkids, vecs, nil)

	sqls := CdcAppendEventsSql(testTblcfg(), "idx-1", 0, buf, sizes)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 SQL, got %d", len(sqls))
	}
	blobs := extractUnhexBlobs(t, sqls[0])
	chunks := []EventChunk{{ChunkId: 0, Data: blobs[0]}}
	state, err := ReplayEventLog(chunks, dim, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Deleted) != 0 || len(state.Overflow) != 3 {
		t.Fatalf("got deleted=%v overflow=%d, want 0/3", state.Deleted, len(state.Overflow))
	}
	for i, e := range state.Overflow {
		if e.Pkid != pkids[i] {
			t.Fatalf("overflow[%d].Pkid: got %d want %d", i, e.Pkid, pkids[i])
		}
		for k, v := range vecs[i] {
			if math.Float32bits(e.Vec[k]) != math.Float32bits(v) {
				t.Fatalf("overflow[%d].Vec[%d]: got %v want %v", i, k, e.Vec[k], v)
			}
		}
	}
}

// TestCdcAppendEventsSql_Mixed: mixed DELETE/INSERT in one batch packs
// without splitting records across chunks.
func TestCdcAppendEventsSql_Mixed(t *testing.T) {
	dim := 4
	ops := []CdcOp{CdcOpDelete, CdcOpInsert, CdcOpDelete, CdcOpInsert}
	pkids := []int64{5, 7, 5, 9}
	vecs := [][]float32{{1, 2, 3, 4}, {5, 6, 7, 8}}
	buf, sizes := encodeBatch(t, dim, 0, ops, pkids, vecs, nil)

	sqls := CdcAppendEventsSql(testTblcfg(), "idx-1", 0, buf, sizes)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 SQL, got %d", len(sqls))
	}
	blobs := extractUnhexBlobs(t, sqls[0])
	chunks := []EventChunk{{ChunkId: 0, Data: blobs[0]}}
	state, err := ReplayEventLog(chunks, dim, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Replay: DEL 5 → del{5}; INS 7 → ovf{7}; DEL 5 → del{5} (already there);
	// INS 9 → ovf{7,9}. 5 stays deleted, 7 and 9 in overflow.
	if len(state.Deleted) != 1 || state.Deleted[0] != 5 {
		t.Fatalf("deleted: got %v want [5]", state.Deleted)
	}
	if len(state.Overflow) != 2 || state.Overflow[0].Pkid != 7 || state.Overflow[1].Pkid != 9 {
		t.Fatalf("overflow pkids: got %v want [7 9]",
			[]int64{state.Overflow[0].Pkid, state.Overflow[1].Pkid})
	}
}

// TestCdcAppendEventsSql_ChunkPacking: when records overflow a chunk, the
// helper splits at record boundaries and bumps chunk_id.
func TestCdcAppendEventsSql_ChunkPacking(t *testing.T) {
	dim := 4
	insertSize := 9 + 4*dim // 25 bytes
	// Force just over one chunk.
	n := MaxChunkSize/insertSize + 3
	ops := make([]CdcOp, n)
	pkids := make([]int64, n)
	vecs := make([][]float32, n)
	for i := range ops {
		ops[i] = CdcOpInsert
		pkids[i] = int64(i + 1)
		v := make([]float32, dim)
		for k := range v {
			v[k] = float32(i)*float32(dim) + float32(k)
		}
		vecs[i] = v
	}
	buf, sizes := encodeBatch(t, dim, 0, ops, pkids, vecs, nil)

	sqls := CdcAppendEventsSql(testTblcfg(), "idx-1", 5, buf, sizes)
	all := strings.Join(sqls, " ; ")
	blobs := extractUnhexBlobs(t, all)
	if len(blobs) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(blobs))
	}
	if !strings.Contains(all, "'idx-1', 5, ") || !strings.Contains(all, "'idx-1', 6, ") {
		t.Fatalf("expected chunk_ids 5 and 6: %s", all)
	}
	// Chunks must split at record boundaries (each blob is frame+payload;
	// payload size = blob - cdcFrameOverhead must be a multiple of insertSize).
	if (len(blobs[0])-cdcFrameOverhead)%insertSize != 0 ||
		(len(blobs[1])-cdcFrameOverhead)%insertSize != 0 {
		t.Fatalf("chunks not record-aligned: %d, %d", len(blobs[0]), len(blobs[1]))
	}
	chunks := []EventChunk{
		{ChunkId: 5, Data: blobs[0]},
		{ChunkId: 6, Data: blobs[1]},
	}
	state, err := ReplayEventLog(chunks, dim, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Overflow) != n {
		t.Fatalf("got %d overflow entries, want %d", len(state.Overflow), n)
	}
}

// TestReplayEventLog_DeleteInsertDelete is the user's collapse case — the
// flaw the unified-log design exists to fix. With the old two-stream design,
// tag=1 would carry pkid=1 and tag=2 would carry (1, V), leaving id=1 alive
// in the overflow despite the latest event being DELETE. Here we encode the
// same sequence of CDC events as ordered records and assert replay produces
// deleted={1}, overflow=∅.
func TestReplayEventLog_DeleteInsertDelete(t *testing.T) {
	dim := 4
	ops := []CdcOp{CdcOpDelete, CdcOpInsert, CdcOpDelete}
	pkids := []int64{1, 1, 1}
	vecs := [][]float32{{1, 2, 3, 4}}
	buf, _ := encodeBatch(t, dim, 0, ops, pkids, vecs, nil)

	chunks := []EventChunk{{ChunkId: 0, Data: FrameCdcChunk(buf)}}
	state, err := ReplayEventLog(chunks, dim, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Deleted) != 1 || state.Deleted[0] != 1 {
		t.Fatalf("deleted: got %v want [1]", state.Deleted)
	}
	if len(state.Overflow) != 0 {
		t.Fatalf("overflow should be empty, got %d entries", len(state.Overflow))
	}
}

// TestReplayEventLog_InsertDeleteInsert: opposite collapse — a final INSERT
// after a DELETE wins. Common case: re-INSERT of a previously-deleted pkid.
func TestReplayEventLog_InsertDeleteInsert(t *testing.T) {
	dim := 2
	ops := []CdcOp{CdcOpInsert, CdcOpDelete, CdcOpInsert}
	pkids := []int64{7, 7, 7}
	vecs := [][]float32{{1, 1}, {9, 9}}
	buf, _ := encodeBatch(t, dim, 0, ops, pkids, vecs, nil)

	state, err := ReplayEventLog([]EventChunk{{ChunkId: 0, Data: FrameCdcChunk(buf)}}, dim, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Deleted) != 0 {
		t.Fatalf("deleted should be empty, got %v", state.Deleted)
	}
	if len(state.Overflow) != 1 || state.Overflow[0].Pkid != 7 {
		t.Fatalf("overflow: got %v want one entry pkid=7", state.Overflow)
	}
	// Last INSERT's vec wins.
	if state.Overflow[0].Vec[0] != 9 || state.Overflow[0].Vec[1] != 9 {
		t.Fatalf("vec: got %v want [9 9]", state.Overflow[0].Vec)
	}
}

// TestReplayEventLog_MultiChunk: multi-chunk replay sorts chunks by chunk_id
// before replaying — events in chunk 0 happen before events in chunk 1 even
// when the loader returns them in arbitrary SELECT order.
func TestReplayEventLog_MultiChunk(t *testing.T) {
	dim := 2
	// chunk 0: INSERT pkid=1
	buf0, _ := encodeBatch(t, dim, 0,
		[]CdcOp{CdcOpInsert}, []int64{1}, [][]float32{{1, 1}}, nil)
	// chunk 1: DELETE pkid=1
	buf1, _ := encodeBatch(t, dim, 0,
		[]CdcOp{CdcOpDelete}, []int64{1}, nil, nil)

	// Hand them to ReplayEventLog reversed; SortChunks should normalize.
	chunks := []EventChunk{
		{ChunkId: 1, Data: FrameCdcChunk(buf1)},
		{ChunkId: 0, Data: FrameCdcChunk(buf0)},
	}
	SortChunks(chunks)
	state, err := ReplayEventLog(chunks, dim, 0)
	if err != nil {
		t.Fatal(err)
	}
	// INSERT at chunk 0 then DELETE at chunk 1 → deleted={1}, overflow=∅.
	if len(state.Deleted) != 1 || state.Deleted[0] != 1 {
		t.Fatalf("deleted: got %v want [1]", state.Deleted)
	}
	if len(state.Overflow) != 0 {
		t.Fatalf("overflow should be empty, got %v", state.Overflow)
	}
}

// TestReplayEventLog_WithInclude: include bytes round-trip through the log.
func TestReplayEventLog_WithInclude(t *testing.T) {
	dim := 2
	includeBytesPerRow := 4 + 1 // int32 + 1 mask byte
	include := make([]byte, includeBytesPerRow)
	binary.LittleEndian.PutUint32(include[0:4], 0x12345678)
	include[4] = 0
	buf, _ := encodeBatch(t, dim, includeBytesPerRow,
		[]CdcOp{CdcOpInsert},
		[]int64{42},
		[][]float32{{1, 2}},
		[][]byte{include},
	)
	state, err := ReplayEventLog([]EventChunk{{ChunkId: 0, Data: FrameCdcChunk(buf)}}, dim, includeBytesPerRow)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Overflow) != 1 || len(state.Overflow[0].Include) != includeBytesPerRow {
		t.Fatalf("overflow shape unexpected: %v", state.Overflow)
	}
	if binary.LittleEndian.Uint32(state.Overflow[0].Include[:4]) != 0x12345678 {
		t.Fatalf("include bytes not preserved: %x", state.Overflow[0].Include)
	}
}

// TestReplayEventLog_RejectsCorruptFrame: any framing-level corruption
// (short input, bad magic, wrong version, length mismatch, CRC mismatch,
// or bad end magic) must cause replay to fail loudly rather than produce
// silently-wrong state.
func TestReplayEventLog_RejectsCorruptFrame(t *testing.T) {
	dim := 4
	buf, _ := encodeBatch(t, dim, 0,
		[]CdcOp{CdcOpDelete}, []int64{1}, nil, nil)
	good := FrameCdcChunk(buf)

	type corruption struct {
		name string
		mut  func([]byte) []byte
	}
	cases := []corruption{
		{"too-short", func(b []byte) []byte { return b[:cdcFrameOverhead-1] }},
		{"bad-start-magic", func(b []byte) []byte { c := append([]byte(nil), b...); c[0] ^= 0xFF; return c }},
		{"unknown-version", func(b []byte) []byte { c := append([]byte(nil), b...); c[4] = 99; return c }},
		{"len-overrun", func(b []byte) []byte {
			c := append([]byte(nil), b...)
			binary.LittleEndian.PutUint32(c[8:12], uint32(len(b))) // bogus
			return c
		}},
		{"crc-flip", func(b []byte) []byte {
			c := append([]byte(nil), b...)
			c[cdcHeaderSize] ^= 0xFF // flip a payload byte; CRC will mismatch
			return c
		}},
		{"bad-end-magic", func(b []byte) []byte {
			c := append([]byte(nil), b...)
			c[len(c)-1] ^= 0xFF
			return c
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReplayEventLog([]EventChunk{{ChunkId: 7, Data: tc.mut(good)}}, dim, 0)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
	// Sanity: the unmodified frame round-trips.
	state, err := ReplayEventLog([]EventChunk{{ChunkId: 0, Data: good}}, dim, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Deleted) != 1 || state.Deleted[0] != 1 {
		t.Fatalf("baseline round-trip failed: deleted=%v", state.Deleted)
	}
}

// TestCdcIncludeBytesPerRow covers the helper that derives per-row size from
// a colMetaJSON. Empty / [] → 0; one int64 col → 8 + 1; mixed types add up.
func TestCdcIncludeBytesPerRow(t *testing.T) {
	cases := []struct {
		json string
		want int
	}{
		{"", 0},
		{"[]", 0},
		// 1 int64 column → 8 bytes data + 1 byte null mask = 9
		{`[{"name":"a","type":1}]`, 9},
		// int32 (4) + int64 (8) → 12 + 1 mask byte
		{`[{"name":"a","type":0},{"name":"b","type":1}]`, 13},
		// 9 cols → ceil(9/8)=2 mask bytes
		{`[{"name":"a","type":0},{"name":"b","type":0},{"name":"c","type":0},{"name":"d","type":0},{"name":"e","type":0},{"name":"f","type":0},{"name":"g","type":0},{"name":"h","type":0},{"name":"i","type":0}]`, 9*4 + 2},
	}
	for _, tc := range cases {
		got, err := CdcIncludeBytesPerRow(tc.json)
		if err != nil {
			t.Fatalf("%q: %v", tc.json, err)
		}
		if got != tc.want {
			t.Fatalf("%q: got %d want %d", tc.json, got, tc.want)
		}
	}
	// Unknown type is rejected.
	if _, err := CdcIncludeBytesPerRow(`[{"name":"x","type":99}]`); err == nil {
		t.Fatal("expected error on unknown type")
	}
}

// TestSplitIncludeBytes round-trips column splitting against the writer's
// row-major layout.
func TestSplitIncludeBytes(t *testing.T) {
	colJSON := `[{"name":"a","type":0},{"name":"b","type":1}]` // int32, int64
	includeBytesPerRow := 4 + 8 + 1
	nrows := uint64(3)
	include := make([]byte, int(nrows)*includeBytesPerRow)

	// row 0: a=10, b=100, no nulls
	binary.LittleEndian.PutUint32(include[0:4], 10)
	binary.LittleEndian.PutUint64(include[4:12], 100)
	include[12] = 0
	// row 1: a=20, b=NULL (bit 1)
	binary.LittleEndian.PutUint32(include[13:17], 20)
	include[25] = 0x02
	// row 2: a=NULL (bit 0), b=300
	binary.LittleEndian.PutUint64(include[30:38], 300)
	include[38] = 0x01

	cols, nulls, err := SplitIncludeBytes(colJSON, include, nrows, includeBytesPerRow)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || len(nulls) != 2 {
		t.Fatalf("unexpected col/null counts: %d/%d", len(cols), len(nulls))
	}
	if int(binary.LittleEndian.Uint32(cols[0][0:4])) != 10 {
		t.Fatalf("col0 row0: got %d", binary.LittleEndian.Uint32(cols[0][0:4]))
	}
	if int(binary.LittleEndian.Uint32(cols[0][4:8])) != 20 {
		t.Fatalf("col0 row1: got %d", binary.LittleEndian.Uint32(cols[0][4:8]))
	}
	if binary.LittleEndian.Uint64(cols[1][0:8]) != 100 {
		t.Fatalf("col1 row0: got %d", binary.LittleEndian.Uint64(cols[1][0:8]))
	}
	if binary.LittleEndian.Uint64(cols[1][16:24]) != 300 {
		t.Fatalf("col1 row2: got %d", binary.LittleEndian.Uint64(cols[1][16:24]))
	}
	if nulls[0] == nil || nulls[0][0]&(1<<2) == 0 {
		t.Fatalf("col0 null bitmap missing row 2: %v", nulls[0])
	}
	if nulls[0][0]&^(uint32(1<<2)) != 0 {
		t.Fatalf("col0 null bitmap should only have bit 2: %x", nulls[0][0])
	}
	if nulls[1] == nil || nulls[1][0]&(1<<1) == 0 {
		t.Fatalf("col1 null bitmap missing row 1: %v", nulls[1])
	}
}

func TestSplitIncludeBytes_NoNulls(t *testing.T) {
	colJSON := `[{"name":"a","type":1}]`
	includeBytesPerRow := 8 + 1
	nrows := uint64(2)
	include := make([]byte, int(nrows)*includeBytesPerRow)
	binary.LittleEndian.PutUint64(include[0:8], 7)
	include[8] = 0
	binary.LittleEndian.PutUint64(include[9:17], 13)
	include[17] = 0
	cols, nulls, err := SplitIncludeBytes(colJSON, include, nrows, includeBytesPerRow)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 || cols[0] == nil {
		t.Fatal("expected non-nil col data")
	}
	if nulls[0] != nil {
		t.Fatalf("expected nil null bitmap when no nulls, got %v", nulls[0])
	}
}

func TestNextChunkIdSql(t *testing.T) {
	got := NextChunkIdSql(testTblcfg(), "idx-1", Tag_CdcEvents)
	want := fmt.Sprintf(
		"SELECT COALESCE(MAX(chunk_id) + 1, 0) FROM `db`.`__cuvs_index` WHERE index_id = 'idx-1' AND tag = %d",
		Tag_CdcEvents)
	if got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}

func TestCdcLoadEventsSql(t *testing.T) {
	got := CdcLoadEventsSql(testTblcfg(), "idx-1")
	want := fmt.Sprintf(
		"SELECT chunk_id, data FROM `db`.`__cuvs_index` WHERE index_id = 'idx-1' AND tag = %d",
		Tag_CdcEvents)
	if got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// ResolveIncludeColumns + EncodeIncludeRow (Phase 3.5 CDC INCLUDE support)
// ---------------------------------------------------------------------------

// nameToPosMap is a tiny helper for the tests below.
func nameToPosMap(names ...string) map[string]int32 {
	m := make(map[string]int32, len(names))
	for i, n := range names {
		m[n] = int32(i)
	}
	return m
}

// constTypes returns a colTypeID function backed by a fixed slice. The
// position->type mapping matches the source-table column layout the
// writer sees at construction.
func constTypes(typeIDs ...int32) func(int32) int32 {
	return func(pos int32) int32 {
		if int(pos) < 0 || int(pos) >= len(typeIDs) {
			return -1
		}
		return typeIDs[int(pos)]
	}
}

func TestResolveIncludeColumns_Empty(t *testing.T) {
	bindings, colMeta, ibpr, err := ResolveIncludeColumns("", nameToPosMap("pk", "v"), constTypes(23, 30))
	if err != nil {
		t.Fatal(err)
	}
	if bindings != nil || colMeta != "" || ibpr != 0 {
		t.Fatalf("empty include cols should return zero values: %v %q %d", bindings, colMeta, ibpr)
	}

	// Whitespace-only also yields empty.
	bindings, _, _, err = ResolveIncludeColumns("   ", nameToPosMap("pk"), constTypes(23))
	if err != nil || bindings != nil {
		t.Fatalf("whitespace-only should be empty, got err=%v bindings=%v", err, bindings)
	}
}

func TestResolveIncludeColumns_SingleInt64(t *testing.T) {
	// pk(int64=23) at pos 0; category(int64=23) at pos 1.
	bindings, colMeta, ibpr, err := ResolveIncludeColumns(
		"category",
		nameToPosMap("pk", "category"),
		constTypes(23, 23),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 1 || bindings[0].Name != "category" || bindings[0].Pos != 1 ||
		bindings[0].TypeCode != 1 || bindings[0].SizeBytes != 8 {
		t.Fatalf("unexpected binding: %#v", bindings)
	}
	if !strings.Contains(colMeta, `"name":"category"`) || !strings.Contains(colMeta, `"type":1`) {
		t.Fatalf("unexpected colMeta: %s", colMeta)
	}
	// 8 bytes data + 1 byte null mask = 9.
	if ibpr != 9 {
		t.Fatalf("includeBytesPerRow: got %d, want 9", ibpr)
	}
}

func TestResolveIncludeColumns_MultiMixedTypes(t *testing.T) {
	// pk(int64) at 0; i32 col(int32=22) at 1; f32 col(float32=30) at 2;
	// f64 col(float64=31) at 3; u64 col(uint64=28) at 4.
	bindings, _, ibpr, err := ResolveIncludeColumns(
		"i32, f32, f64, u64",
		nameToPosMap("pk", "i32", "f32", "f64", "u64"),
		constTypes(23, 22, 30, 31, 28),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 4 {
		t.Fatalf("want 4 bindings, got %d", len(bindings))
	}
	wantCodes := []int{0, 2, 3, 4}
	wantSizes := []int{4, 4, 8, 8}
	for i, b := range bindings {
		if b.TypeCode != wantCodes[i] || b.SizeBytes != wantSizes[i] {
			t.Fatalf("binding %d: got type=%d size=%d, want type=%d size=%d",
				i, b.TypeCode, b.SizeBytes, wantCodes[i], wantSizes[i])
		}
	}
	// 4+4+8+8 data + ceil(4/8)=1 null mask = 25.
	if ibpr != 25 {
		t.Fatalf("includeBytesPerRow: got %d, want 25", ibpr)
	}
}

func TestResolveIncludeColumns_UnknownColumn(t *testing.T) {
	_, _, _, err := ResolveIncludeColumns(
		"nosuch",
		nameToPosMap("pk", "v"),
		constTypes(23, 30),
	)
	if err == nil || !strings.Contains(err.Error(), `column "nosuch" not found`) {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

func TestResolveIncludeColumns_UnsupportedType(t *testing.T) {
	// String type (not int/float/uint64) at pos 1.
	const someUnsupportedTypeID = 12 // T_varchar or similar; not in the supported list
	_, _, _, err := ResolveIncludeColumns(
		"name",
		nameToPosMap("pk", "name"),
		constTypes(23, someUnsupportedTypeID),
	)
	if err == nil || !strings.Contains(err.Error(), "unsupported INCLUDE type") {
		t.Fatalf("expected unsupported-type error, got %v", err)
	}
}

func TestEncodeIncludeRow_Empty(t *testing.T) {
	got, err := EncodeIncludeRow(nil, []any{1, 2, 3}, 0)
	if err != nil || got != nil {
		t.Fatalf("empty bindings should return nil bytes, got err=%v len=%d", err, len(got))
	}
}

func TestEncodeIncludeRow_AllTypes(t *testing.T) {
	bindings := []IncludeBinding{
		{Name: "i32", Pos: 1, TypeCode: 0, SizeBytes: 4},
		{Name: "i64", Pos: 2, TypeCode: 1, SizeBytes: 8},
		{Name: "f32", Pos: 3, TypeCode: 2, SizeBytes: 4},
		{Name: "f64", Pos: 4, TypeCode: 3, SizeBytes: 8},
		{Name: "u64", Pos: 5, TypeCode: 4, SizeBytes: 8},
	}
	ibpr := 4 + 8 + 4 + 8 + 8 + 1 // 5 cols + 1 null-mask byte

	row := []any{
		"pk-ignored",
		int32(0x11223344),
		int64(-1),
		float32(1.5),
		float64(2.5),
		uint64(0xFFFFFFFFFFFFFFFF),
	}
	out, err := EncodeIncludeRow(bindings, row, ibpr)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != ibpr {
		t.Fatalf("byte length: got %d, want %d", len(out), ibpr)
	}
	if binary.LittleEndian.Uint32(out[0:4]) != 0x11223344 {
		t.Fatalf("i32 bytes wrong")
	}
	if int64(binary.LittleEndian.Uint64(out[4:12])) != -1 {
		t.Fatalf("i64 bytes wrong")
	}
	if math.Float32frombits(binary.LittleEndian.Uint32(out[12:16])) != 1.5 {
		t.Fatalf("f32 bytes wrong")
	}
	if math.Float64frombits(binary.LittleEndian.Uint64(out[16:24])) != 2.5 {
		t.Fatalf("f64 bytes wrong")
	}
	if binary.LittleEndian.Uint64(out[24:32]) != 0xFFFFFFFFFFFFFFFF {
		t.Fatalf("u64 bytes wrong")
	}
	// null mask: no nulls, last byte must be 0.
	if out[ibpr-1] != 0 {
		t.Fatalf("null mask should be 0, got %#x", out[ibpr-1])
	}
}

func TestEncodeIncludeRow_NullsSetBits(t *testing.T) {
	bindings := []IncludeBinding{
		{Name: "a", Pos: 0, TypeCode: 1, SizeBytes: 8},
		{Name: "b", Pos: 1, TypeCode: 1, SizeBytes: 8},
		{Name: "c", Pos: 2, TypeCode: 1, SizeBytes: 8},
	}
	ibpr := 8*3 + 1
	// a is non-null; b is NULL; c is NULL.
	row := []any{int64(7), nil, nil}
	out, err := EncodeIncludeRow(bindings, row, ibpr)
	if err != nil {
		t.Fatal(err)
	}
	// Bit 1 + bit 2 set in the mask => 0b00000110 = 0x06.
	if out[ibpr-1] != 0x06 {
		t.Fatalf("null mask: got %#x, want 0x06", out[ibpr-1])
	}
	if int64(binary.LittleEndian.Uint64(out[0:8])) != 7 {
		t.Fatalf("non-null value wrong")
	}
}

func TestEncodeIncludeRow_TypeMismatch(t *testing.T) {
	bindings := []IncludeBinding{
		{Name: "i32", Pos: 0, TypeCode: 0, SizeBytes: 4},
	}
	row := []any{"not an int32"}
	_, err := EncodeIncludeRow(bindings, row, 4+1)
	if err == nil || !strings.Contains(err.Error(), "expected int32") {
		t.Fatalf("expected type mismatch error, got %v", err)
	}
}

func TestEncodeIncludeRow_LargerMask(t *testing.T) {
	// 9 cols → null mask is 2 bytes (ceil(9/8)).
	bindings := make([]IncludeBinding, 9)
	for i := range bindings {
		bindings[i] = IncludeBinding{Name: fmt.Sprintf("c%d", i), Pos: int32(i), TypeCode: 1, SizeBytes: 8}
	}
	ibpr := 8*9 + 2
	row := make([]any, 9)
	for i := range row {
		row[i] = nil // all nulls
	}
	out, err := EncodeIncludeRow(bindings, row, ibpr)
	if err != nil {
		t.Fatal(err)
	}
	maskStart := ibpr - 2
	// Bits 0-7 = 0xFF (first byte), bit 8 = 0x01 (second byte).
	if out[maskStart] != 0xFF || out[maskStart+1] != 0x01 {
		t.Fatalf("null mask bytes: got %#x %#x, want 0xFF 0x01", out[maskStart], out[maskStart+1])
	}
}
