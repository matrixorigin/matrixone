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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"math"

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
		if entry.Time.GT(&p.ts) {
			// not visible
			continue
		}
		if entry.RowID.EQ(&p.lastRowID) {
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
	ts                types.TS
	spec              PrimaryKeyMatchSpec
	iter              btree.IterG[*PrimaryIndexEntry]
	rows              *btree.BTreeG[RowEntry]
	primaryIndex      *btree.BTreeG[*PrimaryIndexEntry]
	tombstoneRowIdIdx *btree.BTreeG[*PrimaryIndexEntry]
	curRow            RowEntry

	specHint struct {
		isDelIter bool
	}
}

type PrimaryKeyMatchSpec struct {
	// Next() --> Move() --> moveInner()
	// the existence of Move is to avoid
	// the redundant comparison .
	Move      func(p *primaryKeyIter) bool
	Name      string
	moveInner func(p *primaryKeyIter) bool
}

func Exact(key []byte) PrimaryKeyMatchSpec {
	// if bytes and time matched, we hope scan from the first
	var pivot = PrimaryIndexEntry{
		RowEntryID: math.MaxInt64,
	}

	first := true
	cnt := 3
	spec := PrimaryKeyMatchSpec{
		Name: "Exact",
		moveInner: func(p *primaryKeyIter) bool {
			var ok bool
			if first {
				first = false

				pivot.Bytes = key
				pivot.Time = p.ts

				if ok = p.iter.Seek(&pivot); !ok {
					return false
				}

			} else {
				ok = p.iter.Next()

				if !p.specHint.isDelIter {
					cnt--
				}

				// Note that:
				// delete a pk may happen twice, means there may have two deletes
				// for one pk both pushed to CN, and they have different TS.
				// this happened when an update or delete commit to TN during flush,
				// the delete target is transferred to a new object, so the extra delete was generated.
				// the new delete may have bigger TS than the older one.
				//
				// update case:
				//    delete pk  t1
				//    insert pk  t1
				//
				//    flush ----> transfer delete ---> delete pk t2
				//
				// if the Exact only consider the latest one records, it may miss the insert!
				//
				if !ok || cnt <= 0 {
					return false
				}
			}
			return true
		},
	}

	spec.Move = func(p *primaryKeyIter) bool {
		var ok bool
		for {
			ok = spec.moveInner(p)
			if !ok {
				return false
			}

			if p.specHint.isDelIter != p.iter.Item().Deleted {
				continue
			}

			item := p.iter.Item()
			return bytes.Equal(item.Bytes, key)
		}
	}

	return spec
}

func Prefix(prefix []byte) PrimaryKeyMatchSpec {

	// if bytes and time matched, we hope scan from the first
	var pivot = PrimaryIndexEntry{
		RowEntryID: math.MaxInt64,
	}

	first := true
	spec := PrimaryKeyMatchSpec{
		Name: "Prefix",
		moveInner: func(p *primaryKeyIter) bool {
			var ok bool
			if first {
				first = false

				pivot.Bytes = prefix
				pivot.Time = p.ts

				ok = p.iter.Seek(&pivot)
			} else {
				ok = p.iter.Next()
			}

			if !ok {
				return false
			}

			return true
		},
	}

	spec.Move = func(p *primaryKeyIter) bool {
		var ok bool
		for {
			if ok = spec.moveInner(p); !ok {
				return false
			}

			if p.specHint.isDelIter != p.iter.Item().Deleted {
				continue
			}

			item := p.iter.Item()
			return bytes.HasPrefix(item.Bytes, prefix)
		}
	}

	return spec
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

	// if bytes and time matched, we hope scan from the first
	var pivot = PrimaryIndexEntry{
		RowEntryID: math.MaxInt64,
	}

	first := true
	spec := PrimaryKeyMatchSpec{
		Name: "Between Kind",
		moveInner: func(p *primaryKeyIter) bool {
			var ok bool
			if first {
				first = false

				pivot.Bytes = lb
				pivot.Time = p.ts

				if ok = p.iter.Seek(&pivot); ok {
					ok = seek2First(&p.iter)
				}
			} else {
				ok = p.iter.Next()
			}

			if !ok {
				return false
			}

			return true
		},
	}

	spec.Move = func(p *primaryKeyIter) bool {
		var ok bool
		for {
			if ok = spec.moveInner(p); !ok {
				return false
			}

			if p.specHint.isDelIter != p.iter.Item().Deleted {
				continue
			}

			item := p.iter.Item()
			return validCheck(item.Bytes)
		}
	}

	return spec
}

type phase int

const (
	scan phase = 0
	seek phase = 1
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
				ok = p.iter.Seek(&PrimaryIndexEntry{
					Bytes: lb,
					Time:  p.ts,
					// if bytes and time matched, we hope scan from the first
					RowEntryID: math.MaxInt64,
				})

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

	idx := 0
	vecLen := len(encodes)
	currentPhase := seek

	var match func(key, ee []byte) bool
	if kind == function.PREFIX_IN {
		match = bytes.HasPrefix
	} else {
		match = bytes.Equal
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

	// if bytes and time matched, we hope scan from the first
	var pivot = PrimaryIndexEntry{
		RowEntryID: math.MaxInt64,
	}

	spec := PrimaryKeyMatchSpec{
		Name: "InKind",
		moveInner: func(p *primaryKeyIter) (ret bool) {
			// TODO: optimize the case where len(encodes) >> p.primaryIndex.Len(), refer to the UT TestPrefixIn
			for {
				switch currentPhase {
				case seek:
					if !updateEncoded() {
						// out of vec
						return false
					}

					pivot.Bytes = encoded
					pivot.Time = p.ts

					if !p.iter.Seek(&pivot) {
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
					currentPhase = seek
				}
			}
		},
	}

	spec.Move = func(p *primaryKeyIter) bool {
		var ok bool
		for {
			if ok = spec.moveInner(p); !ok {
				return false
			}

			if p.specHint.isDelIter != p.iter.Item().Deleted {
				continue
			}

			return true
		}
	}

	return spec
}

var _ RowsIter = new(primaryKeyIter)

func (p *primaryKeyIter) isPKItemValid(pkItem PrimaryIndexEntry) bool {
	iter := p.rows.Iter()
	defer iter.Release()

	var pivot = RowEntry{
		Time:    p.ts,
		BlockID: pkItem.BlockID,
		RowID:   pkItem.RowID,
	}

	// for 1000*10 items:
	// Seek() ≈ 10x compare(pk bytes) ≈ 10x Next()
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		row := iter.Item()

		if !row.RowID.EQ(&pkItem.RowID) {
			break
		}

		if p.specHint.isDelIter && !row.Deleted {
			// pick up deletes, should test each item
			continue

		} else if !p.specHint.isDelIter && row.Deleted {
			// pick up inserts, quick break if found this item deleted already
			break
		}

		if row.ID == pkItem.RowEntryID {
			p.curRow = row
			return true
		}

		return false
	}

	return false
}

func (p *primaryKeyIter) Next() bool {
	for {
		if !p.spec.Move(p) {
			return false
		}

		entry := p.iter.Item()

		if p.isPKItemValid(*entry) {
			return true
		}
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

		if entry.BlockID.Compare(&p.bid) != 0 {
			continue
		}

		if p.isPKItemValid(*entry) {
			return true
		}
	}
}

func (p *PartitionState) NewRowsIter(ts types.TS, blockID *types.Blockid, iterDeleted bool) *rowsIter {
	iter := p.rows.Iter()
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

func buildSpec(op int, keys [][]byte) PrimaryKeyMatchSpec {
	switch op {
	case function.EQUAL:
		return Exact(keys[0])

	case function.PREFIX_EQ:
		return Prefix(keys[0])

	case function.IN, function.PREFIX_IN:
		// // may be it's better to iterate rows instead.
		// if len(f.packed) > 128 {
		// 	return
		// }
		//spec = logtailreplay.InKind(f.packed, f.Op)
		return InKind(keys, op)

	case function.LESS_EQUAL, function.LESS_THAN:
		return LessKind(keys[0], op == function.LESS_EQUAL)

	case function.GREAT_EQUAL, function.GREAT_THAN:
		return GreatKind(keys[0], op == function.GREAT_EQUAL)

	case function.BETWEEN, ioutil.RangeLeftOpen,
		ioutil.RangeRightOpen, ioutil.RangeBothOpen, function.PREFIX_BETWEEN:
		var kind int
		switch op {
		case function.BETWEEN:
			kind = 0
		case ioutil.RangeLeftOpen:
			kind = 1
		case ioutil.RangeRightOpen:
			kind = 2
		case ioutil.RangeBothOpen:
			kind = 3
		case function.PREFIX_BETWEEN:
			kind = 4
		}

		return BetweenKind(keys[0], keys[1], kind)
	}

	panic(fmt.Sprintf("build spec failed, %v, %v", op, keys))
}

func (p *PartitionState) NewPrimaryKeyIter(
	ts types.TS,
	op int,
	keys [][]byte,
) *primaryKeyIter {
	index := p.rowPrimaryKeyIndex
	return &primaryKeyIter{
		ts:           ts,
		spec:         buildSpec(op, keys),
		iter:         index.Iter(),
		primaryIndex: index,
		rows:         p.rows,
	}
}

func (p *PartitionState) NewPrimaryKeyDelIter(
	ts *types.TS,
	bid *types.Blockid,
	op int,
	keys [][]byte,
) *primaryKeyDelIter {
	index := p.rowPrimaryKeyIndex
	delIter := &primaryKeyDelIter{
		primaryKeyIter: primaryKeyIter{
			ts:                *ts,
			spec:              buildSpec(op, keys),
			primaryIndex:      index,
			iter:              index.Iter(),
			rows:              p.rows,
			tombstoneRowIdIdx: p.inMemTombstoneRowIdIndex,
		},
		bid: *bid,
	}

	delIter.specHint.isDelIter = true
	return delIter
}
