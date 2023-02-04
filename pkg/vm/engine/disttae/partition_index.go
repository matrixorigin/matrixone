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

package disttae

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/tidwall/btree"
)

type PartitionIndex struct {
	rowVersions *Map[types.Rowid, Versions[RowRef]]
	rowIDs      *btree.BTreeG[types.Rowid]
	index       *btree.BTreeG[*IndexEntry]
}

type RowRef struct {
	ID     int64
	Batch  *batch.Batch
	Offset int
}

type IndexEntry struct {
	Tuple Tuple
	RowID types.Rowid
	// this is required, because a row's value may change (wow!) and tuple may be different.
	// we need to validate the entry by checking the current version of row ref.
	RowRefID int64
}

func (i *IndexEntry) Less(than *IndexEntry) bool {
	if i.Tuple.Less(than.Tuple) {
		return true
	} else if than.Tuple.Less(i.Tuple) {
		return false
	}
	if res := bytes.Compare(i.RowID[:], than.RowID[:]); res < 0 {
		return true
	} else if res > 0 {
		return false
	}
	if i.RowRefID < than.RowRefID {
		return true
	} else if than.RowRefID < i.RowRefID {
		return false
	}
	return false
}

var nextRowRefID int64

func NewPartitionIndex() *PartitionIndex {
	return &PartitionIndex{
		rowVersions: new(Map[types.Rowid, Versions[RowRef]]),
		rowIDs: btree.NewBTreeG(func(a, b types.Rowid) bool {
			return bytes.Compare(a[:], b[:]) < 0
		}),
		index: btree.NewBTreeG(func(a, b *IndexEntry) bool {
			return a.Less(b)
		}),
	}
}

func (p *PartitionIndex) SetIndex(tuple Tuple, rowID types.Rowid, rowRefID int64) {
	p.index.Set(&IndexEntry{
		Tuple:    tuple,
		RowID:    rowID,
		RowRefID: rowRefID,
	})
}

func (p *PartitionIndex) IterIndex(
	ts timestamp.Timestamp,
	lower Tuple,
	upper Tuple,
) *PartitionIndexIter {
	return &PartitionIndexIter{
		iter:        p.index.Iter(),
		rowVersions: p.rowVersions,
		time:        ts,
		lower:       lower,
		upper:       upper,
	}
}

type PartitionIndexIter struct {
	iter        btree.GenericIter[*IndexEntry]
	rowVersions *Map[types.Rowid, Versions[RowRef]]
	time        timestamp.Timestamp
	lower       Tuple
	upper       Tuple

	firstCalled bool
}

func (t *PartitionIndexIter) Next() bool {
	for {

		if !t.firstCalled {
			t.firstCalled = true
			if !t.iter.First() {
				return false
			}
			if !t.iter.Seek(&IndexEntry{
				Tuple: t.lower,
			}) {
				return false
			}
		} else {
			if !t.iter.Next() {
				return false
			}
		}

		entry := t.iter.Item()

		// check bounds
		if entry.Tuple.Less(t.lower) {
			return false
		}
		if t.upper.Less(entry.Tuple) {
			return false
		}

		// check row ref
		versions, ok := t.rowVersions.Get(entry.RowID)
		if !ok {
			continue
		}
		p := versions.Get(t.time)
		if p == nil {
			continue
		}
		if entry.RowRefID != p.ID {
			continue
		}

		return true
	}
}

func (t *PartitionIndexIter) Entry() *IndexEntry {
	return t.iter.Item()
}

func (t *PartitionIndexIter) Close() error {
	t.iter.Release()
	return nil
}
