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
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

// RowEvent is one source-table change fed to the build sink: an insert (Pk +
// Text) or a delete (Pk, Delete=true). An UPDATE is delivered as a delete of the
// old row followed by an insert of the new one — both for the same Pk.
type RowEvent struct {
	Delete bool
	Pk     any
	Text   []byte
}

// Sink accumulates a batch of row events and produces one segment (the inserted
// rows) plus a delete set. It is the engine-side of the maintenance path: the
// initial full build feeds every source row through one Sink to make a tag=0
// base; each subsequent CDC batch feeds its changes through a Sink to make a
// tag=1 tail segment + the batch's deletes. The Index then merges base + tails +
// the accumulated deletes, and liveness (highest Recency) resolves updates.
//
// A pk that is BOTH deleted and (re)inserted in the same batch is an UPDATE — the
// insert at this batch's recency supersedes older copies via liveness, so no
// delete tombstone is emitted for it. Only a pk that is deleted and NOT
// reinserted becomes a tombstone.
type Sink struct {
	pkType     int32
	tok        tokenizer.Tokenizer
	inserts    []Doc
	insertKeys map[any]bool
	deleteKeys map[any]bool
}

// NewSink creates a sink for a batch. pkType is the source pk's types.T; tok is
// the index's tokenizer (used for both this build and later query tokenization).
func NewSink(pkType int32, tok tokenizer.Tokenizer) *Sink {
	return &Sink{
		pkType:     pkType,
		tok:        tok,
		insertKeys: make(map[any]bool),
		deleteKeys: make(map[any]bool),
	}
}

// Add feeds one row event into the batch.
func (s *Sink) Add(ev RowEvent) {
	key := normalizeKey(ev.Pk)
	if ev.Delete {
		s.deleteKeys[key] = true
		return
	}
	s.inserts = append(s.inserts, Doc{Pk: ev.Pk, Text: ev.Text})
	s.insertKeys[key] = true
}

// Build finalizes the batch into a segment (the inserted rows, at the given
// recency) and the tombstone set (pk key → recency) for pks deleted but not
// reinserted in this batch.
func (s *Sink) Build(id string, recency int64) (*Segment, map[any]int64, error) {
	seg, err := BuildSegmentFromDocs(id, s.pkType, s.inserts, s.tok)
	if err != nil {
		return nil, nil, err
	}
	seg.Recency = recency

	var deletes map[any]int64
	for key := range s.deleteKeys {
		if s.insertKeys[key] {
			continue // reinserted in the same batch → an UPDATE, not a tombstone
		}
		if deletes == nil {
			deletes = make(map[any]int64)
		}
		deletes[key] = recency
	}
	return seg, deletes, nil
}

// MergeDeletes folds several per-batch delete maps into one (latest recency
// wins), for feeding NewIndex.
func MergeDeletes(maps ...map[any]int64) map[any]int64 {
	out := make(map[any]int64)
	for _, m := range maps {
		for key, rec := range m {
			if cur, ok := out[key]; !ok || rec > cur {
				out[key] = rec
			}
		}
	}
	return out
}
