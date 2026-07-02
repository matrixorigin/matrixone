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
	"fmt"
	"os"
	"path/filepath"
)

// defaultTailCapacity floors the streaming segment cap when the caller passes a
// non-positive capacity (should not happen — the sinker sources it from
// max_index_capacity, default 1M — but a 0 would disable sealing and reintroduce
// the OOM). Kept in step with iscp.defaultWandCapacity.
const defaultTailCapacity int64 = 1000000

// TailSegment is one sealed CDC segment spilled to a temp file: the path to its
// framed bytes (FrameSegment output) and the frame length (so the persist step can
// advance chunk_id without re-reading the file first).
type TailSegment struct {
	Path     string
	FrameLen int
}

// TailBuilder streams CDC insert/upsert rows into capacity-capped WAND segments,
// spilling each sealed segment's framed bytes to a temp file the moment it fills —
// so the sinker's peak memory is ONE open segment's postings, not the whole CDC
// stream. This is the fix for RunWand buffering every event: an 88M-row initial
// sync would OOM holding all (pk, text) events in RAM before building.
//
// It mirrors hnsw's HnswSync stream-and-spill: Update() rolls to a new model and
// Unload()s full ones to files mid-stream; Save() persists them at close. Here
// AddBatch() rolls+spills sealed segments; Finish() returns the spilled segment
// files + accumulated deletes for the caller to persist in one txn. Deletes stay a
// single record batch (small — pk only). NOT safe for concurrent use; Cleanup()
// must be called (defer) to remove the temp files.
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
	dir, err := os.MkdirTemp("", "wandtail")
	if err != nil {
		return nil, err
	}
	return &TailBuilder{pkType: pkType, capacity: capacity, tokenize: tokenize, dir: dir}, nil
}

// AddBatch streams one decoded CDC batch: insert/upsert rows are tokenized into the
// open segment (sealed + spilled once it reaches `capacity` docs), deletes are
// collected. Same tokenizer as the search side, so build/query tokens match.
func (t *TailBuilder) AddBatch(cdc *WandCdc) error {
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
			// Seal at doc boundaries: len(pks) is the distinct-doc count so far.
			if int64(len(t.cur.model.pks)) >= t.capacity {
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

// seal finalizes the open segment, frames it, and spills the framed bytes to a temp
// file (freeing the segment's C/Go buffers). A no-op if there's no open segment; an
// all-empty segment (every row had no searchable tokens) is dropped, not spilled.
func (t *TailBuilder) seal() error {
	if t.cur == nil {
		return nil
	}
	model := t.cur.Finish()
	t.cur = nil
	if model.N == 0 {
		model.Free()
		return nil
	}
	framed, err := FrameSegment(model)
	model.Free()
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

// Finish seals the final open segment and returns the spilled segments (in append
// order) plus the accumulated deletes. chunk_id is assigned by the caller at
// persist. Cleanup must be called afterwards.
func (t *TailBuilder) Finish() ([]TailSegment, []DeleteRecord, error) {
	if err := t.seal(); err != nil {
		return nil, nil, err
	}
	return t.segs, t.deletes, nil
}

// Cleanup removes the temp dir and all spilled segment files. Idempotent.
func (t *TailBuilder) Cleanup() {
	if t.dir != "" {
		os.RemoveAll(t.dir)
		t.dir = ""
	}
}
