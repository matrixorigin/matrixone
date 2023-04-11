// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnSegment struct {
	*txnbase.TxnSegment
	entry *catalog.SegmentEntry
	table *txnTable
}

type segmentIt struct {
	sync.RWMutex
	linkIt *common.GenericSortedDListIt[*catalog.SegmentEntry]
	curr   *catalog.SegmentEntry
	table  *txnTable
	err    error
}

type composedSegmentIt struct {
	*segmentIt
	uncommitted *catalog.SegmentEntry
}

func newSegmentItOnSnap(table *txnTable) handle.SegmentIt {
	it := &segmentIt{
		linkIt: table.entry.MakeSegmentIt(true),
		table:  table,
	}
	var err error
	var ok bool
	for it.linkIt.Valid() {
		curr := it.linkIt.Get().GetPayload()
		curr.RLock()
		ok, err = curr.IsVisible(it.table.store.txn, curr.RWMutex)
		if err != nil {
			curr.RUnlock()
			it.err = err
			return it
		}
		if ok {
			curr.RUnlock()
			it.curr = curr
			break
		}
		curr.RUnlock()
		it.linkIt.Next()
	}
	//if table.localSegment != nil {
	//	cit := &composedSegmentIt{
	//		segmentIt:   it,
	//		uncommitted: table.localSegment.entry,
	//	}
	//	return cit
	//}
	return it

}

func newSegmentIt(table *txnTable) handle.SegmentIt {
	it := &segmentIt{
		linkIt: table.entry.MakeSegmentIt(true),
		table:  table,
	}
	var err error
	var ok bool
	for it.linkIt.Valid() {
		curr := it.linkIt.Get().GetPayload()
		curr.RLock()
		ok, err = curr.IsVisible(it.table.store.txn, curr.RWMutex)
		if err != nil {
			curr.RUnlock()
			it.err = err
			return it
		}
		if ok {
			curr.RUnlock()
			it.curr = curr
			break
		}
		curr.RUnlock()
		it.linkIt.Next()
	}
	if table.localSegment != nil {
		cit := &composedSegmentIt{
			segmentIt:   it,
			uncommitted: table.localSegment.entry,
		}
		return cit
	}
	return it
}

func (it *segmentIt) Close() error { return nil }

func (it *segmentIt) GetError() error { return it.err }
func (it *segmentIt) Valid() bool {
	if it.err != nil {
		return false
	}
	return it.linkIt.Valid()
}

func (it *segmentIt) Next() {
	var err error
	var valid bool
	for {
		it.linkIt.Next()
		node := it.linkIt.Get()
		if node == nil {
			it.curr = nil
			break
		}
		entry := node.GetPayload()
		entry.RLock()
		valid, err = entry.IsVisible(it.table.store.txn, entry.RWMutex)
		entry.RUnlock()
		if err != nil {
			it.err = err
			break
		}
		if valid {
			it.curr = entry
			break
		}
	}
}

func (it *segmentIt) GetSegment() handle.Segment {
	return newSegment(it.table, it.curr)
}

func (cit *composedSegmentIt) GetSegment() handle.Segment {
	if cit.uncommitted != nil {
		return newSegment(cit.table, cit.uncommitted)
	}
	return cit.segmentIt.GetSegment()
}

func (cit *composedSegmentIt) Valid() bool {
	if cit.err != nil {
		return false
	}
	if cit.uncommitted != nil {
		return true
	}
	return cit.segmentIt.Valid()
}

func (cit *composedSegmentIt) Next() {
	if cit.uncommitted != nil {
		cit.uncommitted = nil
		return
	}
	cit.segmentIt.Next()
}

func newSegment(table *txnTable, meta *catalog.SegmentEntry) *txnSegment {
	seg := &txnSegment{
		TxnSegment: &txnbase.TxnSegment{
			Txn: table.store.txn,
		},
		table: table,
		entry: meta,
	}
	return seg
}

func (seg *txnSegment) GetMeta() any      { return seg.entry }
func (seg *txnSegment) String() string    { return seg.entry.String() }
func (seg *txnSegment) GetID() types.Uuid { return seg.entry.ID }
func (seg *txnSegment) getDBID() uint64   { return seg.entry.GetTable().GetDB().ID }
func (seg *txnSegment) MakeBlockIt() (it handle.BlockIt) {
	return newBlockIt(seg.table, seg.entry)
}

func (seg *txnSegment) CreateNonAppendableBlock(opts *objectio.CreateBlockOpt) (blk handle.Block, err error) {
	return seg.Txn.GetStore().CreateNonAppendableBlock(seg.getDBID(), seg.entry.AsCommonID(), opts)
}

func (seg *txnSegment) IsUncommitted() bool {
	return seg.entry.IsLocal
}

func (seg *txnSegment) IsAppendable() bool { return seg.entry.IsAppendable() }

func (seg *txnSegment) SoftDeleteBlock(id types.Blockid) (err error) {
	fp := seg.entry.AsCommonID()
	fp.BlockID = id
	return seg.Txn.GetStore().SoftDeleteBlock(seg.getDBID(), fp)
}

func (seg *txnSegment) GetRelation() (rel handle.Relation) {
	return newRelation(seg.table)
}

func (seg *txnSegment) GetBlock(id types.Blockid) (blk handle.Block, err error) {
	fp := seg.entry.AsCommonID()
	fp.BlockID = id
	return seg.Txn.GetStore().GetBlock(seg.getDBID(), fp)
}

func (seg *txnSegment) CreateBlock(is1PC bool) (blk handle.Block, err error) {
	return seg.Txn.GetStore().CreateBlock(seg.getDBID(), seg.entry.GetTable().GetID(), seg.entry.ID, is1PC)
}
