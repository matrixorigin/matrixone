// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

type RowsIter interface {
	Next() bool
	Close() error
	Entry() RowEntry
}

type rowsIter struct {
	ts           types.TS
	iter         btree.IterG[RowEntry]
	firstCalled  bool
	lastRowID    types.Rowid
	checkBlockID bool
	blockID      types.Blockid
	iterDeleted  bool
}

func (p *PartitionState) NewRowsIter(ts types.TS, blockID *types.Blockid, iterDeleted bool) *rowsIter {
	iter := p.rows.Copy().Iter()
	ret := &rowsIter{
		ts:          ts,
		iter:        iter,
		iterDeleted: iterDeleted,
	}
	if blockID != nil {
		ret.checkBlockID = true
		ret.blockID = *blockID
	}
	return ret
}

func (p *PartitionState) NewDirtyRowsIter(
	ts types.TS,
	blockID *types.Blockid) *rowsIter {
	iter := p.dirtyRows.Copy().Iter()
	ret := &rowsIter{
		ts:          ts,
		iter:        iter,
		iterDeleted: true,
	}
	if blockID != nil {
		ret.checkBlockID = true
		ret.blockID = *blockID
	}
	return ret
}

var _ RowsIter = new(rowsIter)

func (p *rowsIter) Next() bool {
	for {

		if !p.firstCalled {
			if p.checkBlockID {
				if !p.iter.Seek(RowEntry{
					BlockID: p.blockID,
				}) {
					return false
				}
			} else {
				if !p.iter.First() {
					return false
				}
			}
			p.firstCalled = true
		} else {
			if !p.iter.Next() {
				return false
			}
		}

		entry := p.iter.Item()

		if p.checkBlockID && entry.BlockID != p.blockID {
			// no more
			return false
		}
		if entry.Time.Greater(p.ts) {
			// not visible
			continue
		}
		if entry.RowID.Equal(p.lastRowID) {
			// already met
			continue
		}
		if entry.Deleted != p.iterDeleted {
			// not wanted, skip to next row id
			p.lastRowID = entry.RowID
			continue
		}

		p.lastRowID = entry.RowID
		return true
	}
}

func (p *rowsIter) Entry() RowEntry {
	return p.iter.Item()
}

func (p *rowsIter) Close() error {
	p.iter.Release()
	return nil
}

type primaryKeyIter struct {
	ts          types.TS
	key         []byte
	iter        btree.IterG[*PrimaryIndexEntry]
	firstCalled bool
	rows        *btree.BTreeG[RowEntry]
	curRow      RowEntry
}

func (p *PartitionState) NewPrimaryKeyIter(ts types.TS, key []byte) *primaryKeyIter {
	iter := p.primaryIndex.Copy().Iter()
	return &primaryKeyIter{
		ts:   ts,
		key:  key,
		iter: iter,
		rows: p.rows.Copy(),
	}
}

var _ RowsIter = new(primaryKeyIter)

func (p *primaryKeyIter) Next() bool {
	for {

		if !p.firstCalled {
			if !p.iter.Seek(&PrimaryIndexEntry{
				Bytes: p.key,
			}) {
				return false
			}
			p.firstCalled = true
		} else {
			if !p.iter.Next() {
				return false
			}
		}

		entry := p.iter.Item()

		if !bytes.Equal(entry.Bytes, p.key) {
			// no more
			return false
		}

		// validate
		valid := false
		rowsIter := p.rows.Iter()
		for ok := rowsIter.Seek(RowEntry{
			BlockID: entry.BlockID,
			RowID:   entry.RowID,
			Time:    p.ts,
		}); ok; ok = rowsIter.Next() {
			row := rowsIter.Item()
			if row.BlockID != entry.BlockID {
				// no more
				break
			}
			if row.RowID != entry.RowID {
				// no more
				break
			}
			if row.Time.Greater(p.ts) {
				// not visible
				continue
			}
			valid = row.ID == entry.RowEntryID
			if valid {
				p.curRow = row
			}
			break
		}
		rowsIter.Release()

		if !valid {
			continue
		}

		return true
	}
}

func (p *primaryKeyIter) Entry() RowEntry {
	return p.curRow
}

func (p *primaryKeyIter) Close() error {
	p.iter.Release()
	return nil
}
