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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnSegment struct {
	*txnbase.TxnSegment
	entry *catalog.SegmentEntry
	store txnif.TxnStore
}

type segmentIt struct {
	sync.RWMutex
	txn    txnif.AsyncTxn
	linkIt *common.LinkIt
	curr   *catalog.SegmentEntry
}

func newSegmentIt(txn txnif.AsyncTxn, meta *catalog.TableEntry) *segmentIt {
	it := &segmentIt{
		txn:    txn,
		linkIt: meta.MakeSegmentIt(true),
	}
	for it.linkIt.Valid() {
		curr := it.linkIt.Get().GetPayload().(*catalog.SegmentEntry)
		curr.RLock()
		if curr.TxnCanRead(it.txn, curr.RWMutex) {
			curr.RUnlock()
			it.curr = curr
			break
		}
		curr.RUnlock()
		it.linkIt.Next()
	}
	return it
}

func (it *segmentIt) Close() error { return nil }

func (it *segmentIt) Valid() bool { return it.linkIt.Valid() }

func (it *segmentIt) Next() {
	valid := true
	for {
		it.linkIt.Next()
		node := it.linkIt.Get()
		if node == nil {
			it.curr = nil
			break
		}
		entry := node.GetPayload().(*catalog.SegmentEntry)
		entry.RLock()
		valid = entry.TxnCanRead(it.txn, entry.RWMutex)
		entry.RUnlock()
		if valid {
			it.curr = entry
			break
		}
	}
}

func (it *segmentIt) GetSegment() handle.Segment {
	return newSegment(it.txn, it.curr)
}

func newSegment(txn txnif.AsyncTxn, meta *catalog.SegmentEntry) *txnSegment {
	seg := &txnSegment{
		TxnSegment: &txnbase.TxnSegment{
			Txn: txn,
		},
		entry: meta,
	}
	return seg
}

func (seg *txnSegment) GetMeta() interface{} { return seg.entry }
func (seg *txnSegment) String() string       { return seg.entry.String() }
func (seg *txnSegment) GetID() uint64        { return seg.entry.GetID() }
func (seg *txnSegment) MakeBlockIt() (it handle.BlockIt) {
	return newBlockIt(seg.Txn, seg.entry)
}

func (seg *txnSegment) CreateNonAppendableBlock() (blk handle.Block, err error) {
	return seg.Txn.GetStore().CreateNonAppendableBlock(seg.entry.AsCommonID())
}

func (seg *txnSegment) SoftDeleteBlock(id uint64) (err error) {
	fp := seg.entry.AsCommonID()
	fp.BlockID = id
	return seg.Txn.GetStore().SoftDeleteBlock(fp)
}

func (seg *txnSegment) GetRelation() (rel handle.Relation) {
	return newRelation(seg.Txn, seg.entry.GetTable())
}

func (seg *txnSegment) GetBlock(id uint64) (blk handle.Block, err error) {
	fp := seg.entry.AsCommonID()
	fp.BlockID = id
	return seg.Txn.GetStore().GetBlock(fp)
}

func (seg *txnSegment) CreateBlock() (blk handle.Block, err error) {
	return seg.Txn.GetStore().CreateBlock(seg.entry.GetTable().GetID(), seg.entry.GetID())
}

func (seg *txnSegment) BatchDedup(pks *vector.Vector) (err error) {
	segData := seg.entry.GetSegmentData()
	seg.Txn.GetStore().LogSegmentID(seg.entry.GetTable().GetID(), seg.entry.GetID())
	return segData.BatchDedup(seg.Txn, pks)
}
