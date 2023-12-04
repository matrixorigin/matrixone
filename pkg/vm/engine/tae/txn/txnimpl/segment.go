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

// var newObjectCnt atomic.Int64
// var getObjectCnt atomic.Int64
// var putObjectCnt atomic.Int64
// func GetStatsString() string {
// 	return fmt.Sprintf(
// 		"NewSeg: %d, GetSeg: %d, PutSeg: %d, NewBlk: %d, GetBlk: %d, PutBlk: %d",
// 		newObjectCnt.Load(),
// 		getObjectCnt.Load(),
// 		putObjectCnt.Load(),
// 		newBlockCnt.Load(),
// 		getBlockCnt.Load(),
// 		putBlockCnt.Load())
// }

var (
	_segPool = sync.Pool{
		New: func() any {
			// newObjectCnt.Add(1)
			return &txnObject{}
		},
	}
)

type txnObject struct {
	txnbase.TxnObject
	entry *catalog.ObjectEntry
	table *txnTable
}

type ObjectIt struct {
	sync.RWMutex
	linkIt *common.GenericSortedDListIt[*catalog.ObjectEntry]
	curr   *catalog.ObjectEntry
	table  *txnTable
	err    error
}

type composedObjectIt struct {
	*ObjectIt
	uncommitted *catalog.ObjectEntry
}

func newObjectItOnSnap(table *txnTable) handle.ObjectIt {
	it := &ObjectIt{
		linkIt: table.entry.MakeObjectIt(true),
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
	return it
}

func newObjectIt(table *txnTable) handle.ObjectIt {
	it := &ObjectIt{
		linkIt: table.entry.MakeObjectIt(true),
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
	if table.localObject != nil {
		cit := &composedObjectIt{
			ObjectIt:    it,
			uncommitted: table.localObject.entry,
		}
		return cit
	}
	return it
}

func (it *ObjectIt) Close() error { return nil }

func (it *ObjectIt) GetError() error { return it.err }
func (it *ObjectIt) Valid() bool {
	if it.err != nil {
		return false
	}
	return it.linkIt.Valid()
}

func (it *ObjectIt) Next() {
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

func (it *ObjectIt) GetObject() handle.Object {
	return newObject(it.table, it.curr)
}

func (cit *composedObjectIt) GetObject() handle.Object {
	if cit.uncommitted != nil {
		return newObject(cit.table, cit.uncommitted)
	}
	return cit.ObjectIt.GetObject()
}

func (cit *composedObjectIt) Valid() bool {
	if cit.err != nil {
		return false
	}
	if cit.uncommitted != nil {
		return true
	}
	return cit.ObjectIt.Valid()
}

func (cit *composedObjectIt) Next() {
	if cit.uncommitted != nil {
		cit.uncommitted = nil
		return
	}
	cit.ObjectIt.Next()
}

func newObject(table *txnTable, meta *catalog.ObjectEntry) *txnObject {
	seg := _segPool.Get().(*txnObject)
	// getObjectCnt.Add(1)
	seg.Txn = table.store.txn
	seg.table = table
	seg.entry = meta
	return seg
}

func (seg *txnObject) reset() {
	seg.entry = nil
	seg.table = nil
	seg.TxnObject.Reset()
}

func (seg *txnObject) Close() (err error) {
	seg.reset()
	_segPool.Put(seg)
	// putObjectCnt.Add(1)
	return
}

func (seg *txnObject) GetMeta() any           { return seg.entry }
func (seg *txnObject) String() string         { return seg.entry.String() }
func (seg *txnObject) GetID() *types.Objectid { return &seg.entry.ID }
func (seg *txnObject) MakeBlockIt() (it handle.BlockIt) {
	return newBlockIt(seg.table, seg.entry)
}

func (seg *txnObject) CreateNonAppendableBlock(opts *objectio.CreateBlockOpt) (blk handle.Block, err error) {
	return seg.Txn.GetStore().CreateNonAppendableBlock(seg.entry.AsCommonID(), opts)
}

func (seg *txnObject) IsUncommitted() bool {
	return seg.entry.IsLocal
}

func (seg *txnObject) IsAppendable() bool { return seg.entry.IsAppendable() }

func (seg *txnObject) SoftDeleteBlock(id types.Blockid) (err error) {
	fp := seg.entry.AsCommonID()
	fp.BlockID = id
	return seg.Txn.GetStore().SoftDeleteBlock(fp)
}

func (seg *txnObject) GetRelation() (rel handle.Relation) {
	return newRelation(seg.table)
}

func (seg *txnObject) GetBlock(id types.Blockid) (blk handle.Block, err error) {
	fp := seg.entry.AsCommonID()
	fp.BlockID = id
	return seg.Txn.GetStore().GetBlock(fp)
}

func (seg *txnObject) CreateBlock(is1PC bool) (blk handle.Block, err error) {
	id := seg.entry.AsCommonID()
	return seg.Txn.GetStore().CreateBlock(id, is1PC)
}

func (seg *txnObject) UpdateStats(stats objectio.ObjectStats) error {
	id := seg.entry.AsCommonID()
	return seg.Txn.GetStore().UpdateObjectStats(id, &stats)
}
