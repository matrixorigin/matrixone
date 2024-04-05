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
	"github.com/reusee/pt"
	"github.com/tidwall/btree"
)

type RowsIter interface {
	Next() bool
	Close() error
	Entry() RowEntry
}

type rowsIter struct {
	ts           types.TS
	iter         *pt.Iter[RowEntry]
	firstCalled  bool
	lastRowID    types.Rowid
	checkBlockID bool
	blockID      types.Blockid
	iterDeleted  bool
	current      RowEntry
}

func (p *PartitionState) NewRowsIter(ts types.TS, blockID *types.Blockid, iterDeleted bool) *rowsIter {
	iter := p.rows.NewIter()
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

var _ RowsIter = new(rowsIter)

func (p *rowsIter) Next() bool {
	for {

		var entry RowEntry
		var ok bool
		if !p.firstCalled {
			if p.checkBlockID {
				entry, ok = p.iter.Seek(RowEntry{
					BlockID: p.blockID,
				})
				if !ok {
					return false
				}
			} else {
				entry, ok = p.iter.Next()
				if !ok {
					return false
				}
			}
			p.firstCalled = true
		} else {
			entry, ok = p.iter.Next()
			if !ok {
				return false
			}
		}
		p.current = entry

		if p.checkBlockID && entry.BlockID != p.blockID {
			// no more
			return false
		}
		if entry.Time.Greater(&p.ts) {
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
	return p.current
}

func (p *rowsIter) Close() error {
	p.iter.Close()
	return nil
}

type primaryKeyIter struct {
	ts          types.TS
	spec        PrimaryKeyMatchSpec
	iter        btree.IterG[*PrimaryIndexEntry]
	firstCalled bool
	rows        *pt.Treap[RowEntry]
	curRow      RowEntry
}

type PrimaryKeyMatchSpec struct {
	Seek  []byte
	Match func(key []byte) bool
}

func Exact(key []byte) PrimaryKeyMatchSpec {
	return PrimaryKeyMatchSpec{
		Seek: key,
		Match: func(k []byte) bool {
			return bytes.Equal(k, key)
		},
	}
}

func Prefix(prefix []byte) PrimaryKeyMatchSpec {
	return PrimaryKeyMatchSpec{
		Seek: prefix,
		Match: func(k []byte) bool {
			return bytes.HasPrefix(k, prefix)
		},
	}
}

func MinMax(min []byte, max []byte) PrimaryKeyMatchSpec {
	return PrimaryKeyMatchSpec{
		Seek: min,
		Match: func(k []byte) bool {
			return bytes.Compare(min, k) <= 0 &&
				bytes.Compare(k, max) <= 0
		},
	}
}

func (p *PartitionState) NewPrimaryKeyIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec,
) *primaryKeyIter {
	iter := p.primaryIndex.Copy().Iter()
	return &primaryKeyIter{
		ts:   ts,
		spec: spec,
		iter: iter,
		rows: p.rows,
	}
}

var _ RowsIter = new(primaryKeyIter)

func (p *primaryKeyIter) Next() bool {
	for {

		if !p.firstCalled {
			if !p.iter.Seek(&PrimaryIndexEntry{
				Bytes: p.spec.Seek,
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

		if !p.spec.Match(entry.Bytes) {
			// no more
			return false
		}

		// validate
		valid := false
		rowsIter := p.rows.NewIter()
		for row, ok := rowsIter.Seek(RowEntry{
			BlockID: entry.BlockID,
			RowID:   entry.RowID,
			Time:    p.ts,
		}); ok; row, ok = rowsIter.Next() {
			if row.BlockID != entry.BlockID {
				// no more
				break
			}
			if row.RowID != entry.RowID {
				// no more
				break
			}
			if row.Time.Greater(&p.ts) {
				// not visible
				continue
			}
			if row.Deleted {
				// visible and deleted, no longer valid
				break
			}
			valid = row.ID == entry.RowEntryID
			if valid {
				p.curRow = row
			}
			break
		}
		rowsIter.Close()

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

type primaryKeyDelIter struct {
	primaryKeyIter
	bid types.Blockid
}

func (p *PartitionState) NewPrimaryKeyDelIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec,
	bid types.Blockid,
) *primaryKeyDelIter {
	iter := p.primaryIndex.Copy().Iter()
	return &primaryKeyDelIter{
		primaryKeyIter: primaryKeyIter{
			ts:   ts,
			spec: spec,
			iter: iter,
			rows: p.rows,
		},
		bid: bid,
	}
}

var _ RowsIter = new(primaryKeyDelIter)

func (p *primaryKeyDelIter) Next() bool {
	for {

		if !p.firstCalled {
			if !p.iter.Seek(&PrimaryIndexEntry{
				Bytes: p.spec.Seek,
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

		if !p.spec.Match(entry.Bytes) {
			// no more
			return false
		}

		if entry.BlockID.Compare(p.bid) != 0 {
			continue
		}

		// validate
		valid := false
		rowsIter := p.rows.NewIter()
		for row, ok := rowsIter.Seek(RowEntry{
			BlockID: entry.BlockID,
			RowID:   entry.RowID,
			Time:    p.ts,
		}); ok; row, ok = rowsIter.Next() {
			if row.BlockID != entry.BlockID {
				// no more
				break
			}
			if row.RowID != entry.RowID {
				// no more
				break
			}
			if row.Time.Greater(&p.ts) {
				// not visible
				continue
			}
			if !row.Deleted {
				// skip not deleted
				continue
			}
			valid = row.ID == entry.RowEntryID
			if valid {
				p.curRow = row
			}
			break
		}
		rowsIter.Close()

		if !valid {
			continue
		}

		return true
	}
}
