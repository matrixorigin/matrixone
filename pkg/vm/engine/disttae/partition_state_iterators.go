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

package disttae

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

type partitionStateIter interface {
	Next() bool
	Close() error
	Entry() RowEntry
}

type partitionStateRowsIter struct {
	ts           types.TS
	iter         btree.IterG[RowEntry]
	firstCalled  bool
	lastRowID    types.Rowid
	checkBlockID bool
	blockID      types.Blockid
	iterDeleted  bool
}

func (p *PartitionState) NewRowsIter(ts types.TS, blockID *types.Blockid, iterDeleted bool) *partitionStateRowsIter {
	iter := p.Rows.Copy().Iter()
	ret := &partitionStateRowsIter{
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

func (p *partitionStateRowsIter) Next() bool {
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

func (p *partitionStateRowsIter) Entry() RowEntry {
	return p.iter.Item()
}

func (p *partitionStateRowsIter) Close() error {
	p.iter.Release()
	return nil
}

type partitionStatePrimaryKeyIter struct {
	ts          types.TS
	key         []byte
	iter        btree.IterG[*PrimaryIndexEntry]
	firstCalled bool
	rows        *btree.BTreeG[RowEntry]
	curRow      RowEntry
}

func (p *PartitionState) NewPrimaryKeyIter(ts types.TS, key []byte) *partitionStatePrimaryKeyIter {
	iter := p.PrimaryIndex.Copy().Iter()
	return &partitionStatePrimaryKeyIter{
		ts:   ts,
		key:  key,
		iter: iter,
		rows: p.Rows.Copy(),
	}
}

func (p *partitionStatePrimaryKeyIter) Next() bool {
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

func (p *partitionStatePrimaryKeyIter) Entry() RowEntry {
	return p.curRow
}

func (p *partitionStatePrimaryKeyIter) Close() error {
	p.iter.Release()
	return nil
}
