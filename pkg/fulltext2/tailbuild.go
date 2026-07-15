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
const defaultTailCapacity int64 = 1000000

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
// whole CDC stream. Deletes accumulate as a single (pk-only) batch. It mirrors
// bm25's TailBuilder (positional Builder here). NOT safe for concurrent use;
// Cleanup() must be deferred to remove the temp files.
type TailBuilder struct {
	pkType   int32
	capacity int64
	tokenize func(string) []string
	dir      string
	seq      int
	cur      *Builder // current open segment (nil until the first insert row)
	segs     []TailSegment
	deletes  []DeleteRecord
}

// NewTailBuilder creates a streaming tail builder backed by a private temp dir.
func NewTailBuilder(pkType int32, capacity int64, tokenize func(string) []string) (*TailBuilder, error) {
	if capacity < 1 {
		capacity = defaultTailCapacity
	}
	dir, err := os.MkdirTemp("", "ftv2tail")
	if err != nil {
		return nil, err
	}
	return &TailBuilder{pkType: pkType, capacity: capacity, tokenize: tokenize, dir: dir}, nil
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
				t.cur = NewBuilder(fmt.Sprintf("cdctail-%d", t.seq), t.pkType)
			}
			for _, w := range t.tokenize(e.Text) {
				if err := t.cur.Add(w, e.Pk); err != nil {
					return err
				}
			}
			if int64(t.cur.NumDocs()) >= t.capacity {
				if err := t.seal(); err != nil {
					return err
				}
			}
		case cdcDelete:
			t.deletes = append(t.deletes, DeleteRecord{Pk: e.Pk})
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

// Finish seals the final open segment, frames + spills the accumulated delete
// batch, and returns ALL spilled frame files in chunk_id order: the DELETE frame
// FIRST (lowest chunk_id), so a same-batch UPDATE's new insert segment — at a
// higher chunk_id — supersedes the deleted base copy under Index liveness. Each is
// a temp file the caller persists via load_file; chunk_id is assigned at persist.
// Cleanup must be called afterwards.
func (t *TailBuilder) Finish() ([]TailSegment, error) {
	if err := t.seal(); err != nil {
		return nil, err
	}
	if len(t.deletes) == 0 {
		return t.segs, nil
	}
	framed, err := FrameDeletes(t.pkType, t.deletes)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(t.dir, "delete.frame")
	if err := os.WriteFile(path, framed, 0o600); err != nil {
		return nil, err
	}
	return append([]TailSegment{{Path: path, FrameLen: len(framed)}}, t.segs...), nil
}

// Cleanup removes the temp dir and all spilled segment files. Idempotent.
func (t *TailBuilder) Cleanup() {
	if t.dir != "" {
		os.RemoveAll(t.dir)
		t.dir = ""
	}
}
