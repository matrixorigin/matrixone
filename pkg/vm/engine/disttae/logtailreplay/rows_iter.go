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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
	firstCalled  bool
	rows         *btree.BTreeG[RowEntry]
	primaryIndex *btree.BTreeG[*PrimaryIndexEntry]
	curRow       RowEntry
}

type PrimaryKeyMatchSpec struct {
	// Move moves to the target
	Move  func(p *primaryKeyIter) bool
	Name  string
	Debug bool
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

type phase int

const (
	scan phase = 0
	seek phase = 1
)

func ExactIn(vec *vector.Vector, debug bool, packerPool *fileservice.Pool[*types.Packer]) PrimaryKeyMatchSpec {
	var packer *types.Packer
	var encoded []byte
	var put fileservice.PutBack[*types.Packer]

	idx := 0
	currentPhase := seek
	vecLen := vec.Length()

	updateEncoded := func() bool {
		if idx >= vecLen {
			return false
		}
		vecItem := vec.GetRawBytesAt(idx)
		put = packerPool.Get(&packer)
		encoded = EncodePrimaryKey(vecItem, packer)
		packer.Reset()
		put.Put()
		//encoded = vecItem
		idx++
		return true
	}

	match := func(key []byte) bool {
		return bytes.Equal(key, encoded)
	}

	return PrimaryKeyMatchSpec{
		Name: "ExactIn",
		Move: func(p *primaryKeyIter) bool {
			//var buf *bytes.Buffer
			//if debug {
			//	fmt.Println(vec.GetRawBytesAt(0), fmt.Sprintf("%s", string(vec.GetRawBytesAt(0))))
			//	p.primaryIndex.Scan(func(item *PrimaryIndexEntry) bool {
			//		fmt.Printf("%v-%s", item.Bytes, string(item.Bytes))
			//		return true
			//	})
			//	fmt.Println()
			//	fmt.Println()
			//	buf = &bytes.Buffer{}
			//	defer func() {
			//		fmt.Println("buffer: ", buf.String())
			//	}()
			//}

			for {
				switch currentPhase {
				case seek:
					if !updateEncoded() {
						// out of vec
						return false
					}
					if !p.iter.Seek(&PrimaryIndexEntry{Bytes: encoded}) {
						return false
					}
					if match(p.iter.Item().Bytes) {
						currentPhase = scan
						return true
					}
					continue

				case scan:
					if !p.iter.Next() {
						return false
					}
					if match(p.iter.Item().Bytes) {
						return true
					}
					// seek next vec item
					currentPhase = seek
				}
			}
		},
	}
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
			rows: p.rows.Copy(),
		},
		bid: bid,
	}
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
