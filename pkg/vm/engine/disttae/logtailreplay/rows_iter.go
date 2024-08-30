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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
	ts           types.TS
	spec         PrimaryKeyMatchSpec
	iter         btree.IterG[*PrimaryIndexEntry]
	rows         *btree.BTreeG[RowEntry]
	primaryIndex *btree.BTreeG[*PrimaryIndexEntry]
	curRow       RowEntry
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
				ok = p.iter.Seek(&PrimaryIndexEntry{
					Bytes: key,
				})
			} else {
				ok = p.iter.Next()
			}

			if !ok {
				return false
			}

			item := p.iter.Item()
			return bytes.Equal(item.Bytes, key)
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
				ok = p.iter.Seek(&PrimaryIndexEntry{
					Bytes: prefix,
				})
			} else {
				ok = p.iter.Next()
			}

			if !ok {
				return false
			}

			item := p.iter.Item()
			return bytes.HasPrefix(item.Bytes, prefix)
		},
	}
}

func MinMax(min []byte, max []byte) PrimaryKeyMatchSpec {
	return PrimaryKeyMatchSpec{}
}

func BetweenKind(lb, ub []byte, kind int) PrimaryKeyMatchSpec {
	// 0: [,]
	// 1: (,]
	// 2: [,)
	// 3: (,)
	// 4: prefix between
	var validCheck func(bb []byte) bool
	var seek2First func(iter *btree.IterG[*PrimaryIndexEntry]) bool
	switch kind {
	case 0:
		validCheck = func(bb []byte) bool {
			return bytes.Compare(bb, ub) <= 0
		}
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool { return true }
	case 1:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) <= 0 }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool {
			for bytes.Equal(iter.Item().Bytes, lb) {
				if ok := iter.Next(); !ok {
					return false
				}
			}
			return true
		}
	case 2:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) < 0 }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool { return true }
	case 3:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) < 0 }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool {
			for bytes.Equal(iter.Item().Bytes, lb) {
				if ok := iter.Next(); !ok {
					return false
				}
			}
			return true
		}
	case 4:
		validCheck = func(bb []byte) bool { return types.PrefixCompare(bb, ub) <= 0 }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool { return true }
	default:
		logutil.Infof("between kind missed: kind: %d, lb=%v, ub=%v\n", kind, lb, ub)
		validCheck = func(bb []byte) bool { return true }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool { return true }
	}

	first := true
	return PrimaryKeyMatchSpec{
		Name: "Between Kind",
		Move: func(p *primaryKeyIter) bool {
			var ok bool
			if first {
				first = false
				if ok = p.iter.Seek(&PrimaryIndexEntry{Bytes: lb}); ok {
					ok = seek2First(&p.iter)
				}
			} else {
				ok = p.iter.Next()
			}

			if !ok {
				return false
			}

			item := p.iter.Item()
			return validCheck(item.Bytes)
		},
	}
}

type phase int

const (
	scan  phase = 0
	seek  phase = 1
	judge phase = 2
)

func LessKind(ub []byte, closed bool) PrimaryKeyMatchSpec {
	first := true
	return PrimaryKeyMatchSpec{
		Move: func(p *primaryKeyIter) bool {
			var ok bool
			if first {
				first = false
				ok = p.iter.First()
				return ok
			}

			ok = p.iter.Next()
			if !ok {
				return false
			}

			if closed {
				return bytes.Compare(p.iter.Item().Bytes, ub) <= 0
			}

			return bytes.Compare(p.iter.Item().Bytes, ub) < 0
		},
	}
}

func GreatKind(lb []byte, closed bool) PrimaryKeyMatchSpec {
	// a > x
	// a >= x
	first := true
	return PrimaryKeyMatchSpec{
		Move: func(p *primaryKeyIter) bool {
			var ok bool
			if first {
				first = false
				ok = p.iter.Seek(&PrimaryIndexEntry{Bytes: lb})

				for ok && !closed && bytes.Equal(p.iter.Item().Bytes, lb) {
					ok = p.iter.Next()
				}
				return ok
			}

			return p.iter.Next()
		},
	}
}

func InKind(encodes [][]byte, kind int) PrimaryKeyMatchSpec {
	var encoded []byte

	first := true
	iterateAll := false

	idx := 0
	vecLen := len(encodes)
	currentPhase := seek

	match := func(key, ee []byte) bool {
		if kind == function.PREFIX_IN {
			return bytes.HasPrefix(key, ee)
		} else {
			// in
			return bytes.Equal(key, ee)
		}
	}

	var prev []byte = nil
	updateEncoded := func() bool {
		if idx == 0 && idx < vecLen {
			prev = encodes[idx]
			encoded = encodes[idx]
			idx++
			return true
		}

		for idx < vecLen && match(encodes[idx], prev) {
			idx++
		}

		if idx >= vecLen {
			return false
		}

		// not match
		prev = encodes[idx]
		encoded = encodes[idx]
		idx++
		return true
	}

	return PrimaryKeyMatchSpec{
		Name: "InKind",
		Move: func(p *primaryKeyIter) (ret bool) {
			if first {
				first = false
				// each seek may visit height items
				// we choose to scan all if the seek is more expensive
				if len(encodes)*p.primaryIndex.Height() > p.primaryIndex.Len() {
					iterateAll = true
				}
			}

			for {
				switch currentPhase {
				case judge:
					if iterateAll {
						if !updateEncoded() {
							return false
						}
						currentPhase = scan
					} else {
						currentPhase = seek
					}

				case seek:
					if !updateEncoded() {
						// out of vec
						return false
					}
					if !p.iter.Seek(&PrimaryIndexEntry{Bytes: encoded}) {
						return false
					}
					if match(p.iter.Item().Bytes, encoded) {
						currentPhase = scan
						return true
					}

				case scan:
					if !p.iter.Next() {
						return false
					}
					if match(p.iter.Item().Bytes, encoded) {
						return true
					}
					p.iter.Prev()
					currentPhase = judge
				}
			}
		},
	}
}

var _ RowsIter = new(primaryKeyIter)

func (p *primaryKeyIter) Next() bool {
	for {
		if !p.spec.Move(p) {
			return false
		}

		entry := p.iter.Item()

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

type primaryKeyDelIter struct {
	primaryKeyIter
	bid types.Blockid
}

var _ RowsIter = new(primaryKeyDelIter)

func (p *primaryKeyDelIter) Next() bool {
	for {
		if !p.spec.Move(&p.primaryKeyIter) {
			return false
		}

		entry := p.iter.Item()

		if entry.BlockID.Compare(p.bid) != 0 {
			continue
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
		rowsIter.Release()

		if !valid {
			continue
		}

		return true
	}
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

func (p *PartitionState) NewPrimaryKeyIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec,
) *primaryKeyIter {
	index := p.primaryIndex.Copy()
	return &primaryKeyIter{
		ts:           ts,
		spec:         spec,
		iter:         index.Iter(),
		primaryIndex: index,
		rows:         p.rows.Copy(),
	}
}

//type primaryKeyDelIter struct {
//	primaryKeyIter
//	bid types.Blockid
//}

func (p *PartitionState) NewPrimaryKeyDelIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec,
	bid types.Blockid,
) *primaryKeyDelIter {
	index := p.primaryIndex.Copy()
	return &primaryKeyDelIter{
		primaryKeyIter: primaryKeyIter{
			ts:           ts,
			spec:         spec,
			primaryIndex: index,
			iter:         index.Iter(),
			rows:         p.rows.Copy(),
		},
		bid: bid,
	}
}
