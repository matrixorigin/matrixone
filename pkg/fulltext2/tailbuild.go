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
	"fmt"
	"os"
	"path/filepath"
)

// defaultTailCapacity floors the streaming segment cap when a non-positive
// capacity is passed (the sinker sources it from max_index_capacity, default 1M).
const defaultTailCapacity int64 = DefaultBuildCapacity

// TailSegment locates one sealed CDC frame inside a PACKED spool file: Path is the spool file,
// Offset is the frame's start byte within it, and FrameLen its length. Many frames share one spool
// file (rolled at DefaultTailSpoolRollBytes), so a burst of tiny CDC frames costs a handful of files
// instead of one file per frame; persist reads exactly [Offset, Offset+FrameLen) via load_file.
type TailSegment struct {
	Path     string
	Offset   int64
	FrameLen int
}

// TailBuilder streams CDC insert/upsert rows into capacity-capped positional segments and DELETEs
// into capacity-capped pk-only frames, spilling each sealed frame's bytes to a temp file the moment
// it fills — so the sinker's peak memory is ONE open segment's postings plus one open delete run,
// never the whole CDC stream.
//
// Frames are sealed, and returned by Finish, in CHRONOLOGICAL order (the order op-runs arrive), so
// chunk_id — assigned sequentially at persist and used as the segment recency — reflects operation
// order. This gives last-writer-wins across CDC blobs WITHOUT any unbounded per-session set: a
// later DELETE frame outranks an earlier INSERT segment of the same pk (delete wins), and a
// re-INSERT after a DELETE outranks it (re-insert wins). Switching op type seals the currently-open
// structure first (see AddBatch), which is what preserves that order; the producer flushes a new
// blob per op change, so this seals at blob granularity, not per event. Mirrors bm25's TailBuilder
// (positional Builder here). NOT safe for concurrent use; Cleanup() must be deferred to remove the
// temp files.
//
// Each sealed frame is APPENDED to a rolling packed spool file (rolled at rollCap), not written to
// its own file — so an alternating delete/insert stream (which seals one frame per op run) costs a
// bounded number of files, O(total frame bytes / rollCap), instead of one file per op run (which
// could reach hundreds of thousands over a 30-minute iteration and exhaust inodes). The frame
// ORDER, count, and per-frame chunk_id are unchanged — only the file storage is packed — so
// last-writer-wins recency is byte-for-byte identical to a file-per-frame layout.
type TailBuilder struct {
	pkType     int32
	capacity   int64 // doc cap (max_index_capacity)
	postingCap int64 // posting cap (max_postings_capacity); seal on whichever is hit first
	tokenize   func(string) []WordPos
	dir        string
	seq        int
	delSeq     int
	cur        *Builder       // open INSERT segment (nil between insert runs)
	delBatch   []DeleteRecord // open DELETE run (sealed on an op switch to insert, at capacity, or at Finish)
	frames     []TailSegment  // sealed frames (insert segs + delete frames) in chronological (recency) order

	// Packed spool: sealed frames are appended to curFile at curOff until it would exceed rollCap,
	// then a fresh file is opened. curFile is closed on roll and in Finish/Cleanup so persist's
	// load_file reads flushed bytes. fileSeq names the spool files.
	rollCap int64
	curFile *os.File
	curPath string
	curOff  int64
	fileSeq int

	opts         []BuildOpt // carried into each per-segment Builder (e.g. WithPositionFree)
	includeTypes []int32    // INCLUDE column schema, adopted from the first CDC batch
}

// NewTailBuilder creates a streaming tail builder backed by a private temp dir under
// spillDir (the LOCAL fileservice's __fulltext2 dir; "" ⇒ the OS temp dir, for tests /
// no-fileservice callers). Pass WithPositionFree() so a position-free index's CDC tail
// stays position-free. capacity is max_index_capacity (docs), postingCap is
// max_postings_capacity (postings); each non-positive value falls back to its default.
func NewTailBuilder(pkType int32, capacity, postingCap int64, spillDir string, tokenize func(string) []WordPos, opts ...BuildOpt) (*TailBuilder, error) {
	if capacity < 1 {
		capacity = defaultTailCapacity
	}
	if postingCap < 1 {
		postingCap = DefaultPostingCapacity
	}
	dir, err := os.MkdirTemp(spillDir, "ftv2tail")
	if err != nil {
		return nil, err
	}
	return &TailBuilder{pkType: pkType, capacity: capacity, postingCap: postingCap, tokenize: tokenize, dir: dir, rollCap: DefaultTailSpoolRollBytes, opts: opts}, nil
}

// DefaultTailSpoolRollBytes caps one packed spool file; appending a frame that would exceed it
// starts a fresh file (a single frame larger than the cap gets its own file). Bounds a temp file's
// size while packing many small CDC frames into few files instead of one file per frame.
const DefaultTailSpoolRollBytes int64 = 128 << 20 // 128 MiB

// writeFrame appends framed to the current rolling spool file — starting a new file when the
// current one would exceed rollCap — and returns the file path plus the frame's byte offset within
// it, so persist can address exactly [offset, offset+len) via load_file. A frame is never split
// across files, so load_file always sees a contiguous range.
func (t *TailBuilder) writeFrame(framed []byte) (string, int64, error) {
	if t.curFile != nil && t.curOff > 0 && t.curOff+int64(len(framed)) > t.rollCap {
		if err := t.curFile.Close(); err != nil {
			return "", 0, err
		}
		t.curFile = nil
	}
	if t.curFile == nil {
		path := filepath.Join(t.dir, fmt.Sprintf("spool-%d.frames", t.fileSeq))
		t.fileSeq++
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return "", 0, err
		}
		t.curFile, t.curPath, t.curOff = f, path, 0
	}
	off := t.curOff
	if _, err := t.curFile.Write(framed); err != nil {
		return "", 0, err
	}
	t.curOff += int64(len(framed))
	return t.curPath, off, nil
}

// AddBatch streams one decoded CDC batch: insert/upsert rows are tokenized into
// the open segment (sealed + spilled once it reaches `capacity` docs), deletes are
// collected. Same tokenizer as the search side, so build/query tokens match.
func (t *TailBuilder) AddBatch(cdc *Cdc) error {
	// The CDC blob is self-describing: adopt its INCLUDE schema once so every tail segment
	// this builder seals carries the docmap include section (WithIncludeTypes). A copy of
	// t.opts avoids mutating the caller's slice.
	if t.includeTypes == nil && len(cdc.IncludeTypes) > 0 {
		t.includeTypes = cdc.IncludeTypes
		t.opts = append(append([]BuildOpt(nil), t.opts...), WithIncludeTypes(t.includeTypes))
	}
	// Process events IN ORDER, sealing at each op-type SWITCH so frames are produced — and later
	// returned by Finish — in chronological order. Recency (chunk_id) then follows operation order
	// across CDC blobs, giving last-writer-wins: switching to an insert seals the open delete run
	// first (so a re-insert outranks the prior delete of that pk); switching to a delete seals the
	// open insert segment first (so the delete outranks the prior insert). The producer flushes a
	// new blob per op change, so this seals at blob granularity, not per event, and both open
	// structures stay bounded to one capacity.
	for i := range cdc.Events {
		e := &cdc.Events[i]
		switch e.Op {
		case cdcInsert, cdcUpsert:
			words := t.tokenize(e.Text)
			// An INSERT of empty / zero-token text has nothing to index and no prior version to
			// shadow, so skip it (matches the base build, which drops empty docs). An UPSERT of
			// empty / NULL text MUST still emit a doc so it shadows the old indexed version —
			// SetDoc writes a live 0-term doc for that.
			if len(words) == 0 && e.Op == cdcInsert {
				continue
			}
			if len(t.delBatch) > 0 { // op switch delete->insert: seal the delete run first (lower recency)
				if err := t.sealDeletes(); err != nil {
					return err
				}
			}
			if t.cur == nil {
				t.cur = NewBuilder(fmt.Sprintf("cdctail-%d", t.seq), t.pkType, t.opts...)
			}
			// REPLACE, not append: a repeated upsert of one pk within this open segment supersedes
			// its earlier terms AND INCLUDE values (Add would merge).
			t.cur.SetDoc(e.Pk, words, e.Include)
			if ReachedSegmentCap(t.cur, t.capacity, t.postingCap) {
				if err := t.seal(); err != nil {
					return err
				}
			}
		case cdcDelete:
			if t.cur != nil { // op switch insert->delete: seal the insert segment first (lower recency)
				if err := t.seal(); err != nil {
					return err
				}
			}
			t.delBatch = append(t.delBatch, DeleteRecord{Pk: e.Pk})
			if int64(len(t.delBatch)) >= t.capacity {
				if err := t.sealDeletes(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// seal finalizes the open segment, frames it, and spills the framed bytes to a
// temp file. No-op with no open segment; an all-empty segment is dropped.
func (t *TailBuilder) seal() error {
	if t.cur == nil {
		return nil
	}
	seg, err := t.cur.Finish()
	t.cur = nil
	if err != nil {
		return err
	}
	if seg.N == 0 {
		return nil
	}
	framed, err := FrameSegment(seg)
	if err != nil {
		return err
	}
	t.seq++
	path, off, err := t.writeFrame(framed)
	if err != nil {
		return err
	}
	t.frames = append(t.frames, TailSegment{Path: path, Offset: off, FrameLen: len(framed)})
	return nil
}

// sealDeletes frames the OPEN delete run and spills it to a temp file, appending it to the
// chronological frame list. Called on an op switch to insert, when the run reaches capacity, and
// at Finish — so tombstones stay bounded to one capacity's worth in the heap (never accumulate
// for the whole session). No-op when the run is empty.
func (t *TailBuilder) sealDeletes() error {
	if len(t.delBatch) == 0 {
		return nil
	}
	framed, err := FrameDeletes(t.pkType, t.delBatch)
	if err != nil {
		return err
	}
	t.delBatch = nil // free the run; the next delete re-grows a fresh slice
	t.delSeq++
	path, off, err := t.writeFrame(framed)
	if err != nil {
		return err
	}
	t.frames = append(t.frames, TailSegment{Path: path, Offset: off, FrameLen: len(framed)})
	return nil
}

// Finish seals the final open insert segment and delete run (in that order — at most one is open,
// since an op switch already sealed the other), CLOSES the current spool file so its bytes are
// flushed for load_file, then returns ALL frames in chronological (recency) order. chunk_id is
// assigned sequentially at persist, so recency = operation order and the last writer wins across
// CDC blobs (see AddBatch / the type doc). Each frame is a [Offset, Offset+FrameLen) range in its
// packed spool file, which the caller persists via load_file. Cleanup must be called afterwards.
func (t *TailBuilder) Finish() ([]TailSegment, error) {
	if err := t.seal(); err != nil {
		return nil, err
	}
	if err := t.sealDeletes(); err != nil {
		return nil, err
	}
	if t.curFile != nil {
		if err := t.curFile.Close(); err != nil {
			return nil, err
		}
		t.curFile = nil
	}
	return t.frames, nil
}

// Cleanup closes any open spool file and removes the temp dir with all packed spool files. Idempotent.
func (t *TailBuilder) Cleanup() {
	if t.curFile != nil {
		t.curFile.Close()
		t.curFile = nil
	}
	if t.dir != "" {
		os.RemoveAll(t.dir)
		t.dir = ""
	}
}
