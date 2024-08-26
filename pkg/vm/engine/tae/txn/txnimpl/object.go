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

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
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
	linkIt btree.IterG[*catalog.ObjectEntry]
	curr   *catalog.ObjectEntry
	table  *txnTable
	err    error
}

type composedObjectIt struct {
	*ObjectIt
	uncommitted *catalog.ObjectEntry
}

func newObjectItOnSnap(table *txnTable, isTombstone bool) handle.ObjectIt {
	it := &ObjectIt{
		linkIt: table.entry.MakeObjectIt(isTombstone),
		table:  table,
	}
	return it
}

func newObjectIt(table *txnTable, isTombstone bool) handle.ObjectIt {
	it := newObjectItOnSnap(table, isTombstone)
	if table.getBaseTable(isTombstone) != nil && table.getBaseTable(isTombstone).tableSpace != nil {
		cit := &composedObjectIt{
			ObjectIt:    it.(*ObjectIt),
			uncommitted: table.getBaseTable(isTombstone).tableSpace.entry,
		}
		return cit
	}
	return it
}

func (it *ObjectIt) Close() error {
	it.linkIt.Release()
	return nil
}

func (it *ObjectIt) GetError() error { return it.err }

func (it *ObjectIt) Next() bool {
	var valid bool
	for {
		if !it.linkIt.Next() {
			return false
		}
		entry := it.linkIt.Item()
		valid = entry.IsVisible(it.table.store.txn)
		if valid {
			it.curr = entry
			return true
		}
	}
}

func (it *ObjectIt) GetObject() handle.Object {
	return newObject(it.table, it.curr)
}

func (cit *composedObjectIt) GetObject() handle.Object {
	return cit.ObjectIt.GetObject()
}

func (cit *composedObjectIt) Next() bool {
	if cit.uncommitted != nil {
		cit.curr = cit.uncommitted
		cit.uncommitted = nil
		return true
	}
	return cit.ObjectIt.Next()
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
func buildObject(table *txnTable, meta *catalog.ObjectEntry) handle.Object {
	return newObject(table, meta)
}
func (obj *txnObject) Close() (err error) {
	obj.reset()
	_objPool.Put(obj)
	// putObjectCnt.Add(1)
	return
}
func (obj *txnObject) GetTotalChanges() int {
	return obj.entry.GetObjectData().GetTotalChanges()
}
func (obj *txnObject) RangeDelete(blkID uint16, start, end uint32, dt handle.DeleteType, mp *mpool.MPool) (err error) {
	schema := obj.table.GetLocalSchema(true)
	pkDef := schema.GetPrimaryKey()
	pkVec := makeWorkspaceVector(pkDef.Type)
	defer pkVec.Close()
	for row := start; row <= end; row++ {
		pkVal, _, err := obj.entry.GetObjectData().GetValue(
			obj.table.store.GetContext(), obj.Txn, schema, blkID, int(row), pkDef.Idx, true, mp,
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
func (obj *txnObject) GetID() *types.Objectid { return obj.entry.ID() }
func (obj *txnObject) BlkCnt() int            { return obj.entry.BlockCnt() }
func (obj *txnObject) IsUncommitted() bool {
	return obj.entry.IsLocal
}

func (obj *txnObject) IsAppendable() bool { return obj.entry.IsAppendable() }

func (obj *txnObject) GetRelation() (rel handle.Relation) {
	return newRelation(obj.table)
}

func (obj *txnObject) UpdateStats(stats objectio.ObjectStats) error {
	id := obj.entry.AsCommonID()
	return obj.Txn.GetStore().UpdateObjectStats(id, &stats, obj.entry.IsTombstone)
}

func (obj *txnObject) Prefetch(idxes []int) error {
	schema := obj.table.GetLocalSchema(obj.entry.IsTombstone)
	seqnums := make([]uint16, 0, len(idxes))
	for _, idx := range idxes {
		if idx == catalog.COLIDX_COMMITS {
			seqnums = append(seqnums, objectio.SEQNUM_COMMITTS)
			continue
		}
		seqnums = append(seqnums, schema.ColDefs[idx].SeqNum)
	}
	if obj.IsUncommitted() {
		return obj.table.dataTable.tableSpace.Prefetch(obj.entry, seqnums)
	}
	for i := 0; i < obj.entry.BlockCnt(); i++ {
		err := obj.entry.GetObjectData().Prefetch(seqnums, uint16(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func (obj *txnObject) Fingerprint() *common.ID { return obj.entry.AsCommonID() }

func (obj *txnObject) UpdateDeltaLoc(blkID uint16, deltaLoc objectio.Location) error {
	id := obj.entry.AsCommonID()
	id.SetBlockOffset(blkID)
	return obj.table.store.UpdateDeltaLoc(id, deltaLoc)
}

func (obj *txnObject) Scan(
	ctx context.Context,
	bat **containers.Batch,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (err error) {
	if obj.entry.IsLocal {
		obj.table.dataTable.tableSpace.Scan(ctx, bat, colIdxes, mp)
		return
	}
	return obj.entry.GetObjectData().Scan(ctx, bat, obj.Txn, obj.table.getSchema(obj.entry.IsTombstone), blkID, colIdxes, mp)
}

func (obj *txnObject) HybridScan(
	ctx context.Context,
	bat **containers.Batch,
	blkOffset uint16,
	colIdxs []int,
	mp *mpool.MPool,
) error {
	if obj.entry.IsLocal {
		obj.table.dataTable.tableSpace.HybridScan(ctx, bat, colIdxs, mp)
		return nil
	}
	blkID := objectio.NewBlockidWithObjectID(obj.GetID(), blkOffset)
	return tables.HybridScanByBlock(
		ctx, obj.entry.GetTable(), obj.Txn, bat, obj.table.getSchema(obj.entry.IsTombstone), colIdxs, blkID, mp)
}
