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

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type RowsIter interface {
	Next() bool
	Close() error
	Entry() RowEntry
}

type RowEntry struct {
	//BlockID types.Blockid // we need to iter by block id, so put it first to allow faster iteration
	RowID types.Rowid
	//PK    []byte
	//Time    types.TS

	//ID                int64 // a unique version id, for primary index building and validating
	Deleted bool
	Batch   *batch.Batch
	Offset  int64
	//PrimaryIndexBytes []byte
}

type rowTombstoneIter struct {
	ts          types.TS
	firstCalled bool
	iter        btree.IterG[ObjTombstoneRowEntry]

	firstIterated bool
	tombstoneIter btree.IterG[TombstoneRowEntry]
	curRow        RowEntry
	//lastRowID    types.Rowid
	checkBlockID bool
	blockID      types.Blockid
}

func (p *PartitionState) NewRowTombstoneIter(
	ts types.TS,
	blockID *types.Blockid) *rowTombstoneIter {
	iter := p.tombstones.Copy().Iter()
	ret := &rowTombstoneIter{
		ts:   ts,
		iter: iter,
	}
	if blockID != nil {
		ret.checkBlockID = true
		ret.blockID = *blockID
	}
	return ret
}

func (p *rowTombstoneIter) Next() bool {

	for {

		for {

			if !p.firstCalled {
				if !p.iter.First() {
					return false
				}
				p.firstCalled = true
				p.tombstoneIter = p.iter.Item().Tombstones.Load().Copy().Iter()
			}

			if p.checkBlockID {
				obj := p.iter.Item().ShortObjName
				if bytes.Compare(objectio.ShortName(&p.blockID)[:], obj[:]) != 0 {
					break
				}
			}

			for {

				if !p.firstIterated {
					if p.checkBlockID {
						if !p.tombstoneIter.Seek(TombstoneRowEntry{
							BlockID: p.blockID,
						}) {
							break
						}
					} else {
						if !p.tombstoneIter.First() {
							break
						}
					}
					p.firstIterated = true
				} else {
					if !p.tombstoneIter.Next() {
						break
					}
				}
				tombstone := p.tombstoneIter.Item()
				//check block id
				if p.checkBlockID && tombstone.BlockID != p.blockID {
					break
				}
				if tombstone.Time.Greater(&p.ts) {
					continue
				}
				p.curRow = RowEntry{
					RowID:   types.BuildRowid(tombstone.BlockID, tombstone.Offset),
					Deleted: true,
				}
				return true

			}

			break
		}

		p.tombstoneIter.Release()
		//next object
		if !p.iter.Next() {
			return false
		}
		p.tombstoneIter = p.iter.Item().Tombstones.Load().Copy().Iter()
		p.firstIterated = false

	}
}

func (p *rowTombstoneIter) Entry() RowEntry {
	return p.curRow
}

func (p *rowTombstoneIter) Close() error {
	p.iter.Release()
	return nil
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

// NewRowsIter creates a new iterator for data rows and tombstones.
func (p *PartitionState) NewRowsIter(ts types.TS) *rowsIter {
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
	return p.iter.Item()
}

func (p *rowsIter) Close() error {
	p.iter.Release()
	return nil
}

type primaryKeyIter struct {
	ts          types.TS
	spec        PrimaryKeyMatchSpec
	firstCalled bool

	objsRowIter  btree.IterG[ObjDataRowEntry]
	objsTombIter btree.IterG[ObjTombstoneRowEntry]

	//iter    btree.IterG[*PrimaryIndexEntry]

	//data row iter for one object.
	dataRowIter btree.IterG[DataRowEntry]
	//data rows for one object.
	dataRows *btree.BTreeG[DataRowEntry]

	//rows       *btree.BTreeG[RowEntry]

	//primaryIndex *btree.BTreeG[*PrimaryIndexEntry]

	curRow RowEntry
}

type PrimaryKeyMatchSpec struct {
	// Move moves to the target
	Move func(p *primaryKeyIter) bool
	Name string
}

func Exact(key []byte) PrimaryKeyMatchSpec {
	first := true
	return PrimaryKeyMatchSpec{
		Name: "Exact",
		Move: func(p *primaryKeyIter) bool {
			var ok bool
			if first {
				first = false
				ok = p.dataRowIter.Seek(DataRowEntry{
					PK: key,
				})
			} else {
				ok = p.dataRowIter.Next()
			}

			if !ok {
				first = true
				return false
			}
			if !bytes.Equal(p.dataRowIter.Item().PK, key) {
				first = true
				return false
			}
			return true
			//item := p.dataRowIter.Item()
			//return bytes.Equal(item.PK, key)
		},
	}
}

func Prefix(prefix []byte) PrimaryKeyMatchSpec {
	first := true
	return PrimaryKeyMatchSpec{
		Name: "Prefix",
		Move: func(p *primaryKeyIter) bool {
			var ok bool
			if first {
				first = false
				ok = p.dataRowIter.Seek(DataRowEntry{
					PK: prefix,
				})
			} else {
				ok = p.dataRowIter.Next()
			}

			if !ok {
				first = true
				return false
			}
			//maybe iter.item > prefix.
			if !bytes.HasPrefix(p.dataRowIter.Item().PK, prefix) {
				first = true
				return false
			}
			return true
			//item := p.dataRowIter.Item()
			//return bytes.HasPrefix(item.PK, prefix)
		},
	}
}

func MinMax(min []byte, max []byte) PrimaryKeyMatchSpec {
	return PrimaryKeyMatchSpec{}
}

type phase int

const (
	scan  phase = 0
	seek  phase = 1
	judge phase = 2
)

func ExactIn(encodes [][]byte) PrimaryKeyMatchSpec {
	var encoded []byte

	first := true
	iterateAll := false

	idx := 0
	vecLen := len(encodes)
	currentPhase := seek

	updateEncoded := func() bool {
		if idx >= vecLen {
			return false
		}
		encoded = encodes[idx]
		idx++
		return true
	}

	match := func(key []byte) bool {
		return bytes.Equal(key, encoded)
	}

	return PrimaryKeyMatchSpec{
		Name: "ExactIn",
		Move: func(p *primaryKeyIter) bool {
			if first {
				first = false
				// each seek may visit height items
				// we choose to scan all if the seek is more expensive
				if len(encodes)*p.dataRows.Height() > p.dataRows.Len() {
					iterateAll = true
				}
			}

			for {
				switch currentPhase {
				case judge:
					if iterateAll {
						if !updateEncoded() {
							first = true
							return false
						}
						currentPhase = scan
					} else {
						currentPhase = seek
					}

				case seek:
					if !updateEncoded() {
						// out of vec
						first = true
						return false
					}
					if !p.dataRowIter.Seek(DataRowEntry{PK: encoded}) {
						first = true
						return false
					}
					if match(p.dataRowIter.Item().PK) {
						currentPhase = scan
						return true
					}

				case scan:
					if !p.dataRowIter.Next() {
						first = true
						return false
					}
					if match(p.dataRowIter.Item().PK) {
						return true
					}
					p.dataRowIter.Prev()
					currentPhase = judge
				}
			}
		},
	}
}

func (p *PartitionState) NewPrimaryKeyIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec,
) *primaryKeyIter {
	return &primaryKeyIter{
		ts:           ts,
		spec:         spec,
		objsRowIter:  p.dataRows.Copy().Iter(),
		objsTombIter: p.tombstones.Copy().Iter(),
	}
}

var _ RowsIter = new(primaryKeyIter)

func (p *primaryKeyIter) Next() bool {

	for {

		for {

			if !p.firstCalled {
				if !p.objsRowIter.First() {
					return false
				}
				p.firstCalled = true
				p.dataRowIter = p.objsRowIter.Item().DataRows.Load().Copy().Iter()
				p.dataRows = p.objsRowIter.Item().DataRows.Load()
			}
			if !p.spec.Move(p) {
				break
			}
			entry := p.dataRowIter.Item()
			//PK is not visible
			if entry.Time.Greater(&p.ts) {
				continue
			}

			rowEntry := RowEntry{
				RowID:  entry.RowID,
				Batch:  entry.Batch,
				Offset: entry.Offset,
			}
			shortName := *objectio.ShortName(entry.RowID.BorrowBlockID())
			ok := p.objsTombIter.Seek(ObjTombstoneRowEntry{
				ShortObjName: shortName,
			})
			if !ok {
				p.curRow = rowEntry
				return true
			}
			item := p.objsTombIter.Item()
			if bytes.Compare(item.ShortObjName[:], shortName[:]) != 0 {
				p.curRow = rowEntry
				return true
			}

			//check tombstone.
			tombstoneIter := item.Tombstones.Load().Copy().Iter()
			ok = tombstoneIter.Seek(TombstoneRowEntry{
				BlockID: entry.RowID.CloneBlockID(),
				Offset:  entry.RowID.GetRowOffset(),
			})
			if !ok {
				p.curRow = rowEntry
				return true
			}
			tombstone := tombstoneIter.Item()
			if tombstone.BlockID != entry.RowID.CloneBlockID() ||
				tombstone.Offset != entry.RowID.GetRowOffset() {
				p.curRow = rowEntry
				return true
			}
			if tombstone.Time.Greater(&p.ts) {
				p.curRow = rowEntry
				return true
			}

		}

		p.dataRowIter.Release()
		//next object
		if !p.objsRowIter.Next() {
			return false
		}
		p.dataRowIter = p.objsRowIter.Item().DataRows.Load().Copy().Iter()
		p.dataRows = p.objsRowIter.Item().DataRows.Load()

	}

}

//func (p *primaryKeyIter) Next() bool {
//	for {
//		if !p.spec.Move(p) {
//			return false
//		}
//
//		entry := p.iter.Item()
//
//		// validate
//		valid := false
//		rowsIter := p.rows.Iter()
//		for ok := rowsIter.Seek(RowEntry{
//			BlockID: entry.BlockID,
//			RowID:   entry.RowID,
//			Time:    p.ts,
//		}); ok; ok = rowsIter.Next() {
//			row := rowsIter.Item()
//			if row.BlockID != entry.BlockID {
//				// no more
//				break
//			}
//			if row.RowID != entry.RowID {
//				// no more
//				break
//			}
//			if row.Time.Greater(&p.ts) {
//				// not visible
//				continue
//			}
//			if row.Deleted {
//				// visible and deleted, no longer valid
//				break
//			}
//			valid = row.ID == entry.RowEntryID
//			if valid {
//				p.curRow = row
//			}
//			break
//		}
//		rowsIter.Release()
//
//		if !valid {
//			continue
//		}
//
//		return true
//	}
//}

func (p *primaryKeyIter) Entry() RowEntry {
	return p.curRow
}

func (p *primaryKeyIter) Close() error {
	p.objsRowIter.Release()
	p.objsTombIter.Release()
	return nil
}

type primaryKeyDelIter struct {
	primaryKeyIter
	bid types.Blockid
}

// NewPrimaryKeyDelIter creates a new iterator to get tombstones by primary key and block id.
func (p *PartitionState) NewPrimaryKeyDelIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec,
	bid types.Blockid,
) *primaryKeyDelIter {
	return &primaryKeyDelIter{
		primaryKeyIter: primaryKeyIter{
			ts:           ts,
			spec:         spec,
			objsRowIter:  p.dataRows.Copy().Iter(),
			objsTombIter: p.tombstones.Copy().Iter(),
		},
		bid: bid,
	}
}

var _ RowsIter = new(primaryKeyDelIter)

func (p *primaryKeyDelIter) Next() bool {

	for {

		for {

			if !p.firstCalled {
				if !p.objsRowIter.First() {
					return false
				}
				p.firstCalled = true
				p.dataRowIter = p.objsRowIter.Item().DataRows.Load().Copy().Iter()
				p.dataRows = p.objsRowIter.Item().DataRows.Load()
			}

			if !p.spec.Move(&p.primaryKeyIter) {
				break
			}

			entry := p.dataRowIter.Item()

			//check whether the current object has tombstones.
			shortName := *objectio.ShortName(entry.RowID.BorrowBlockID())
			ok := p.objsTombIter.Seek(ObjTombstoneRowEntry{
				ShortObjName: shortName,
			})
			if !ok {
				//the current object has no tombstones.
				break

			}
			item := p.objsTombIter.Item()
			if bytes.Compare(item.ShortObjName[:], shortName[:]) != 0 {
				//the current object has no tombstones.
				break
			}

			if entry.RowID.BorrowBlockID().Compare(p.bid) != 0 {
				continue
			}

			//PK is not visible
			//if entry.Time.Greater(&p.ts) {
			//	continue
			//}

			rowEntry := RowEntry{
				RowID:  entry.RowID,
				Batch:  entry.Batch,
				Offset: entry.Offset,
			}

			//check tombstone.
			tombstoneIter := item.Tombstones.Load().Copy().Iter()
			ok = tombstoneIter.Seek(TombstoneRowEntry{
				BlockID: entry.RowID.CloneBlockID(),
				Offset:  entry.RowID.GetRowOffset(),
			})
			if !ok {
				continue
			}
			tombstone := tombstoneIter.Item()
			if tombstone.BlockID != entry.RowID.CloneBlockID() ||
				tombstone.Offset != entry.RowID.GetRowOffset() {
				continue
			}
			if tombstone.Time.Greater(&p.ts) {
				continue
			}

			rowEntry.Deleted = true
			p.curRow = rowEntry
			return true

		}

		p.dataRowIter.Release()
		//to check next object
		if !p.objsRowIter.Next() {
			return false
		}
		p.dataRowIter = p.objsRowIter.Item().DataRows.Load().Copy().Iter()
		p.dataRows = p.objsRowIter.Item().DataRows.Load()

	}

}

//func (p *primaryKeyDelIter) Next() bool {
//	for {
//		if !p.spec.Move(&p.primaryKeyIter) {
//			return false
//		}
//
//		entry := p.iter.Item()
//
//		if entry.BlockID.Compare(p.bid) != 0 {
//			continue
//		}
//
//		// validate
//		valid := false
//		rowsIter := p.rows.Iter()
//		for ok := rowsIter.Seek(RowEntry{
//			BlockID: entry.BlockID,
//			RowID:   entry.RowID,
//			Time:    p.ts,
//		}); ok; ok = rowsIter.Next() {
//			row := rowsIter.Item()
//			if row.BlockID != entry.BlockID {
//				// no more
//				break
//			}
//			if row.RowID != entry.RowID {
//				// no more
//				break
//			}
//			if row.Time.Greater(&p.ts) {
//				// not visible
//				continue
//			}
//			if !row.Deleted {
//				// skip not deleted
//				continue
//			}
//			valid = row.ID == entry.RowEntryID
//			if valid {
//				p.curRow = row
//			}
//			break
//		}
//		rowsIter.Release()
//
//		if !valid {
//			continue
//		}
//
//		return true
//	}
//}
