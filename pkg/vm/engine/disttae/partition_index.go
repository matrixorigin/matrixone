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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/tidwall/btree"
)

type PartitionIndex struct {
	RowIDs *Map[types.Rowid, Versions[RowRef]]
	Index  *TreeIndex
}

type RowRef struct {
	ID     int64
	Batch  *batch.Batch
	Offset int
}

var nextRowRefID int64

func NewPartitionIndex() *PartitionIndex {
	return &PartitionIndex{
		RowIDs: new(Map[types.Rowid, Versions[RowRef]]),
		Index:  NewTreeIndex(),
	}
}

func (p *PartitionIndex) Iter(
	ts timestamp.Timestamp,
	lower Tuple,
	upper Tuple,
) *TreeIndexIter {
	return &TreeIndexIter{
		iter:   p.Index.tree.Iter(),
		rowIDs: p.RowIDs,
		time:   ts,
		lower:  lower,
		upper:  upper,
	}
}

type TreeIndexIter struct {
	iter   btree.GenericIter[*IndexEntry]
	rowIDs *Map[types.Rowid, Versions[RowRef]]
	time   timestamp.Timestamp
	lower  Tuple
	upper  Tuple

	firstCalled bool
}

func (t *TreeIndexIter) Next() bool {
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
		versions, ok := t.rowIDs.Get(entry.RowID)
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

func (t *TreeIndexIter) Entry() *IndexEntry {
	return t.iter.Item()
}

func (t *TreeIndexIter) Close() error {
	t.iter.Release()
	return nil
}
