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

// TailSegment is one sealed CDC segment spilled to a temp file: the path to its
// framed bytes (FrameSegment output) and the frame length (so persist can advance
// chunk_id without re-reading the file). Mirrors bm25's TailSegment.
type TailSegment struct {
	Path     string
	FrameLen int
}

// TailBuilder streams CDC insert/upsert rows into capacity-capped positional
// segments, spilling each sealed segment's framed bytes to a temp file the moment
// it fills — so the sinker's peak memory is ONE open segment's postings, not the
// whole CDC stream. Deletes are likewise spilled in capacity-capped (pk-only) frames
// so a long catch-up's tombstones don't accumulate in the heap. It mirrors bm25's
// TailBuilder (positional Builder here). NOT safe for concurrent use; Cleanup() must
// be deferred to remove the temp files.
type TailBuilder struct {
	pkType     int32
	capacity   int64 // doc cap (max_index_capacity)
	postingCap int64 // posting cap (max_postings_capacity); seal on whichever is hit first
	tokenize   func(string) []WordPos
	dir        string
	seq        int
	cur        *Builder // current open segment (nil until the first insert row)
	segs       []TailSegment
	deletes    []DeleteRecord // OPEN delete batch (spilled once it reaches capacity)
	deleteSegs []TailSegment  // sealed delete frames, spilled to temp files
	delSeq     int
	opts       []BuildOpt // carried into each per-segment Builder (e.g. WithPositionFree)
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
	return &TailBuilder{pkType: pkType, capacity: capacity, postingCap: postingCap, tokenize: tokenize, dir: dir, opts: opts}, nil
}

// AddBatch streams one decoded CDC batch: insert/upsert rows are tokenized into
// the open segment (sealed + spilled once it reaches `capacity` docs), deletes are
// collected. Same tokenizer as the search side, so build/query tokens match.
func (t *TailBuilder) AddBatch(cdc *Cdc) error {
	for i := range cdc.Events {
		e := &cdc.Events[i]
		switch e.Op {
		case cdcInsert, cdcUpsert:
			if t.cur == nil {
				t.cur = NewBuilder(fmt.Sprintf("cdctail-%d", t.seq), t.pkType, t.opts...)
			}
			for _, w := range t.tokenize(e.Text) {
				if err := t.cur.Add(w.Word, w.Pos, e.Pk); err != nil {
					return err
				}
			}
			if ReachedSegmentCap(t.cur, t.capacity, t.postingCap) {
				if err := t.seal(); err != nil {
					return err
				}
			}
		case cdcDelete:
			t.deletes = append(t.deletes, DeleteRecord{Pk: e.Pk})
			if int64(len(t.deletes)) >= t.capacity {
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
	path := filepath.Join(t.dir, fmt.Sprintf("seg-%d.frame", t.seq))
	t.seq++
	if err := os.WriteFile(path, framed, 0o600); err != nil {
		return err
	}
	t.segs = append(t.segs, TailSegment{Path: path, FrameLen: len(framed)})
	return nil
}

// sealDeletes frames the OPEN delete batch and spills it to a temp file the same way
// seal() spills insert segments, so the in-memory tombstone slice stays bounded to
// one capacity's worth of pks even over a long CDC catch-up (which appends every
// delete before Finish). No-op with no pending deletes.
func (t *TailBuilder) sealDeletes() error {
	if len(t.deletes) == 0 {
		return nil
	}
	framed, err := FrameDeletes(t.pkType, t.deletes)
	if err != nil {
		return err
	}
	t.deletes = nil // free the batch; the next delete re-grows a fresh slice
	path := filepath.Join(t.dir, fmt.Sprintf("delete-%d.frame", t.delSeq))
	t.delSeq++
	if err := os.WriteFile(path, framed, 0o600); err != nil {
		return err
	}
	t.deleteSegs = append(t.deleteSegs, TailSegment{Path: path, FrameLen: len(framed)})
	return nil
}

// Finish seals the final open insert segment and the final delete batch, then returns
// ALL spilled frame files in chunk_id order: EVERY delete frame FIRST (lowest
// chunk_ids), then the insert segments — so a same-batch UPDATE's new insert (a higher
// chunk_id) supersedes the deleted base copy under Index liveness. Spilling deletes
// into several frames instead of one is order-equivalent: every tombstone still sorts
// below every insert. Each is a temp file the caller persists via load_file; chunk_id
// is assigned at persist. Cleanup must be called afterwards.
func (t *TailBuilder) Finish() ([]TailSegment, error) {
	if err := t.seal(); err != nil {
		return nil, err
	}
	if err := t.sealDeletes(); err != nil {
		return nil, err
	}
	if len(t.deleteSegs) == 0 {
		return t.segs, nil
	}
	out := make([]TailSegment, 0, len(t.deleteSegs)+len(t.segs))
	out = append(out, t.deleteSegs...) // all delete frames first (lowest chunk_ids)
	out = append(out, t.segs...)
	return out, nil
}

// Cleanup removes the temp dir and all spilled segment files. Idempotent.
func (t *TailBuilder) Cleanup() {
	if t.dir != "" {
		os.RemoveAll(t.dir)
		t.dir = ""
	}
}
