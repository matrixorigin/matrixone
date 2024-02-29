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
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

// var newObjectCnt atomic.Int64
// var getObjectCnt atomic.Int64
// var putObjectCnt atomic.Int64
// func GetStatsString() string {
// 	return fmt.Sprintf(
// 		"NewObj: %d, GetObj: %d, PutObj: %d, NewBlk: %d, GetBlk: %d, PutBlk: %d",
// 		newObjectCnt.Load(),
// 		getObjectCnt.Load(),
// 		putObjectCnt.Load(),
// 		newBlockCnt.Load(),
// 		getBlockCnt.Load(),
// 		putBlockCnt.Load())
// }

var (
	_objPool = sync.Pool{
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
	if table.tableSpace != nil {
		cit := &composedObjectIt{
			ObjectIt:    it,
			uncommitted: table.tableSpace.entry,
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
	if isSysTableId(it.table.GetID()) {
		return newSysObject(it.table, it.curr)
	}
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
	obj := _objPool.Get().(*txnObject)
	// getObjectCnt.Add(1)
	obj.Txn = table.store.txn
	obj.table = table
	obj.entry = meta
	return obj
}

func (obj *txnObject) reset() {
	obj.entry = nil
	obj.table = nil
	obj.TxnObject.Reset()
}

func (obj *txnObject) Close() (err error) {
	obj.reset()
	_objPool.Put(obj)
	// putObjectCnt.Add(1)
	return
}
func (obj *txnObject) GetTotalChanges() int {
	return obj.entry.GetBlockData().GetTotalChanges()
}
func (obj *txnObject) RangeDelete(blkID uint16, start, end uint32, dt handle.DeleteType, mp *mpool.MPool) (err error) {
	schema := obj.table.GetLocalSchema()
	pkDef := schema.GetPrimaryKey()
	pkVec := makeWorkspaceVector(pkDef.Type)
	defer pkVec.Close()
	for row := start; row <= end; row++ {
		pkVal, _, err := obj.entry.GetBlockData().GetValue(
			obj.table.store.GetContext(), obj.Txn, schema, blkID, int(row), pkDef.Idx, mp,
		)
		if err != nil {
			return err
		}
		pkVec.Append(pkVal, false)
	}
	id := obj.entry.AsCommonID()
	id.SetBlockOffset(blkID)
	return obj.Txn.GetStore().RangeDelete(id, start, end, pkVec, dt)
}
func (obj *txnObject) GetMeta() any           { return obj.entry }
func (obj *txnObject) String() string         { return obj.entry.String() }
func (obj *txnObject) GetID() *types.Objectid { return &obj.entry.ID }
func (obj *txnObject) BlkCnt() int            { return obj.entry.BlockCnt() }
func (obj *txnObject) IsUncommitted() bool {
	return obj.entry.IsLocal
}

func (obj *txnObject) IsAppendable() bool { return obj.entry.IsAppendable() }

func (obj *txnObject) SoftDeleteBlock(id types.Blockid) (err error) {
	fp := obj.entry.AsCommonID()
	fp.BlockID = id
	return obj.Txn.GetStore().SoftDeleteBlock(fp)
}

func (obj *txnObject) GetRelation() (rel handle.Relation) {
	return newRelation(obj.table)
}

func (obj *txnObject) UpdateStats(stats objectio.ObjectStats) error {
	id := obj.entry.AsCommonID()
	return obj.Txn.GetStore().UpdateObjectStats(id, &stats)
}

func (obj *txnObject) Prefetch(idxes []int) error {
	schema := obj.table.GetLocalSchema()
	seqnums := make([]uint16, 0, len(idxes))
	for _, idx := range idxes {
		seqnums = append(seqnums, schema.ColDefs[idx].SeqNum)
	}
	if obj.IsUncommitted() {
		return obj.table.tableSpace.Prefetch(obj.entry, seqnums)
	}
	for i := 0; i < obj.entry.BlockCnt(); i++ {
		err := obj.entry.GetBlockData().Prefetch(seqnums, uint16(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func (obj *txnObject) Fingerprint() *common.ID { return obj.entry.AsCommonID() }

func (obj *txnObject) GetByFilter(
	ctx context.Context, filter *handle.Filter, mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
	return obj.entry.GetBlockData().GetByFilter(ctx, obj.table.store.txn, filter, mp)
}

func (obj *txnObject) GetColumnDataById(
	ctx context.Context, blkID uint16, colIdx int, mp *mpool.MPool,
) (*containers.ColumnView, error) {
	if obj.entry.IsLocal {
		return obj.table.tableSpace.GetColumnDataById(ctx, obj.entry, colIdx, mp)
	}
	return obj.entry.GetBlockData().GetColumnDataById(ctx, obj.Txn, obj.table.GetLocalSchema(), blkID, colIdx, mp)
}

func (obj *txnObject) GetColumnDataByIds(
	ctx context.Context, blkID uint16, colIdxes []int, mp *mpool.MPool,
) (*containers.BlockView, error) {
	if obj.entry.IsLocal {
		return obj.table.tableSpace.GetColumnDataByIds(obj.entry, colIdxes, mp)
	}
	return obj.entry.GetBlockData().GetColumnDataByIds(ctx, obj.Txn, obj.table.GetLocalSchema(), blkID, colIdxes, mp)
}

func (obj *txnObject) GetColumnDataByName(
	ctx context.Context, blkID uint16, attr string, mp *mpool.MPool,
) (*containers.ColumnView, error) {
	schema := obj.table.GetLocalSchema()
	colIdx := schema.GetColIdx(attr)
	if obj.entry.IsLocal {
		return obj.table.tableSpace.GetColumnDataById(ctx, obj.entry, colIdx, mp)
	}
	return obj.entry.GetBlockData().GetColumnDataById(ctx, obj.Txn, schema, blkID, colIdx, mp)
}

func (obj *txnObject) GetColumnDataByNames(
	ctx context.Context, blkID uint16, attrs []string, mp *mpool.MPool,
) (*containers.BlockView, error) {
	schema := obj.table.GetLocalSchema()
	attrIds := make([]int, len(attrs))
	for i, attr := range attrs {
		attrIds[i] = schema.GetColIdx(attr)
	}
	if obj.entry.IsLocal {
		return obj.table.tableSpace.GetColumnDataByIds(obj.entry, attrIds, mp)
	}
	return obj.entry.GetBlockData().GetColumnDataByIds(ctx, obj.Txn, schema, blkID, attrIds, mp)
}

func (blk *txnObject) UpdateDeltaLoc(blkID uint16, deltaLoc objectio.Location) error {
	id := blk.entry.AsCommonID()
	id.SetBlockOffset(blkID)
	return blk.table.store.UpdateDeltaLoc(id, deltaLoc)
}
