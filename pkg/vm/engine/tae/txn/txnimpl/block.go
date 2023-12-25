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
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

// var newBlockCnt atomic.Int64
// var getBlockCnt atomic.Int64
// var putBlockCnt atomic.Int64

var (
	_blockPool = sync.Pool{
		New: func() any {
			// newBlockCnt.Add(1)
			return &txnBlock{}
		},
	}
)

type txnBlock struct {
	txnbase.TxnBlock
	isUncommitted bool
	entry         *catalog.BlockEntry
	table         *txnTable
}

type blockIt struct {
	sync.RWMutex
	linkIt *common.GenericSortedDListIt[*catalog.BlockEntry]
	curr   *catalog.BlockEntry
	table  *txnTable
	err    error
}

type relBlockIt struct {
	sync.RWMutex
	rel      handle.Relation
	ObjectIt handle.ObjectIt
	blockIt  handle.BlockIt
	err      error
}

func newBlockIt(table *txnTable, meta *catalog.ObjectEntry) *blockIt {
	it := &blockIt{
		table:  table,
		linkIt: meta.MakeBlockIt(true),
	}
	var ok bool
	var err error
	for it.linkIt.Valid() {
		curr := it.linkIt.Get().GetPayload()
		curr.RLock()
		ok, err = curr.IsVisible(it.table.store.txn, curr.RWMutex)
		if err != nil {
			curr.RUnlock()
			it.err = err
			break
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

func (it *blockIt) Close() error { return nil }

func (it *blockIt) Valid() bool {
	if it.err != nil {
		return false
	}
	return it.linkIt.Valid()
}

func (it *blockIt) Next() {
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

func (it *blockIt) GetError() error {
	return it.err
}

func (it *blockIt) GetBlock() handle.Block {
	return buildBlock(it.table, it.curr)
}

func buildBlock(table *txnTable, meta *catalog.BlockEntry) handle.Block {
	if isSysTableId(meta.GetObject().GetTable().ID) {
		return newSysBlock(table, meta)
	}
	return newBlock(table, meta)
}

func newBlock(table *txnTable, meta *catalog.BlockEntry) *txnBlock {
	blk := _blockPool.Get().(*txnBlock)
	// getBlockCnt.Add(1)
	blk.Txn = table.store.txn
	blk.entry = meta
	blk.table = table
	blk.isUncommitted = meta.GetObject().IsLocal
	return blk
}

func (blk *txnBlock) Reset() {
	blk.entry = nil
	blk.table = nil
	blk.isUncommitted = false
	blk.TxnBlock.Reset()
}

func (blk *txnBlock) Close() (err error) {
	blk.Reset()
	_blockPool.Put(blk)
	// putBlockCnt.Add(1)
	return
}

func (blk *txnBlock) GetMeta() any { return blk.entry }
func (blk *txnBlock) String() string {
	if blk.isUncommitted {
		return blk.entry.String()
	}
	blkData := blk.entry.GetBlockData()
	return blkData.PPString(common.PPL1, 0, "")
	// return blk.entry.String()
}

func (blk *txnBlock) IsUncommitted() bool {
	return blk.isUncommitted
}
func (blk *txnBlock) GetTotalChanges() int {
	return blk.entry.GetBlockData().GetTotalChanges()
}
func (blk *txnBlock) IsAppendableBlock() bool { return blk.entry.IsAppendable() }
func (blk *txnBlock) ID() types.Blockid       { return blk.entry.ID }
func (blk *txnBlock) Fingerprint() *common.ID { return blk.entry.AsCommonID() }

func (blk *txnBlock) getDBID() uint64 {
	return blk.entry.GetObject().GetTable().GetDB().ID
}

func (blk *txnBlock) RangeDelete(start, end uint32, dt handle.DeleteType, mp *mpool.MPool) (err error) {
	schema := blk.table.GetLocalSchema()
	pkDef := schema.GetPrimaryKey()
	pkVec := makeWorkspaceVector(pkDef.Type)
	defer pkVec.Close()
	for row := start; row <= end; row++ {
		pkVal, _, err := blk.entry.GetBlockData().GetValue(
			blk.table.store.GetContext(), blk.Txn, schema, int(row), pkDef.Idx, mp,
		)
		if err != nil {
			return err
		}
		pkVec.Append(pkVal, false)
	}
	return blk.Txn.GetStore().RangeDelete(blk.entry.AsCommonID(), start, end, pkVec, dt)
}

func (blk *txnBlock) GetMetaLoc() (metaLoc objectio.Location) {
	return blk.entry.GetVisibleMetaLoc(blk.Txn)
}
func (blk *txnBlock) GetDeltaLoc() (deltaloc objectio.Location) {
	return blk.entry.GetVisibleDeltaLoc(blk.Txn)
}
func (blk *txnBlock) UpdateMetaLoc(metaLoc objectio.Location) (err error) {
	blkID := blk.Fingerprint()
	err = blk.Txn.GetStore().UpdateMetaLoc(blkID, metaLoc)
	return
}

func (blk *txnBlock) UpdateDeltaLoc(deltaloc objectio.Location) (err error) {
	blkID := blk.Fingerprint()
	err = blk.Txn.GetStore().UpdateDeltaLoc(blkID, deltaloc)
	return
}

// TODO: temp use coarse rows
func (blk *txnBlock) Rows() int {
	if blk.isUncommitted {
		return blk.table.tableSpace.GetBlockRows(blk.entry)
	}
	return blk.entry.GetBlockData().Rows()
}

func (blk *txnBlock) GetColumnDataByName(
	ctx context.Context, attr string, mp *mpool.MPool,
) (*containers.ColumnView, error) {
	schema := blk.table.GetLocalSchema()
	colIdx := schema.GetColIdx(attr)
	if blk.isUncommitted {
		return blk.table.tableSpace.GetColumnDataById(ctx, blk.entry, colIdx, mp)
	}
	return blk.entry.GetBlockData().GetColumnDataById(ctx, blk.Txn, schema, colIdx, mp)
}

func (blk *txnBlock) GetColumnDataByNames(
	ctx context.Context, attrs []string, mp *mpool.MPool,
) (*containers.BlockView, error) {
	schema := blk.table.GetLocalSchema()
	attrIds := make([]int, len(attrs))
	for i, attr := range attrs {
		attrIds[i] = schema.GetColIdx(attr)
	}
	if blk.isUncommitted {
		return blk.table.tableSpace.GetColumnDataByIds(blk.entry, attrIds, mp)
	}
	return blk.entry.GetBlockData().GetColumnDataByIds(ctx, blk.Txn, schema, attrIds, mp)
}

func (blk *txnBlock) GetDeltaPersistedTS() types.TS {
	return blk.entry.GetDeltaPersistedTS()
}

func (blk *txnBlock) GetColumnDataById(
	ctx context.Context, colIdx int, mp *mpool.MPool,
) (*containers.ColumnView, error) {
	if blk.isUncommitted {
		return blk.table.tableSpace.GetColumnDataById(ctx, blk.entry, colIdx, mp)
	}
	return blk.entry.GetBlockData().GetColumnDataById(ctx, blk.Txn, blk.table.GetLocalSchema(), colIdx, mp)
}

func (blk *txnBlock) GetColumnDataByIds(
	ctx context.Context, colIdxes []int, mp *mpool.MPool,
) (*containers.BlockView, error) {
	if blk.isUncommitted {
		return blk.table.tableSpace.GetColumnDataByIds(blk.entry, colIdxes, mp)
	}
	return blk.entry.GetBlockData().GetColumnDataByIds(ctx, blk.Txn, blk.table.GetLocalSchema(), colIdxes, mp)
}

func (blk *txnBlock) Prefetch(idxes []int) error {
	schema := blk.table.GetLocalSchema()
	seqnums := make([]uint16, 0, len(idxes))
	for _, idx := range idxes {
		seqnums = append(seqnums, schema.ColDefs[idx].SeqNum)
	}
	if blk.isUncommitted {
		return blk.table.tableSpace.Prefetch(blk.entry, seqnums)
	}
	return blk.entry.GetBlockData().Prefetch(seqnums)
}

func (blk *txnBlock) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return blk.Txn.GetStore().LogTxnEntry(blk.getDBID(), blk.entry.GetObject().GetTable().ID, entry, readed)
}

func (blk *txnBlock) GetObject() (obj handle.Object) {
	obj = newObject(blk.table, blk.entry.GetObject())
	return
}

func (blk *txnBlock) GetByFilter(
	ctx context.Context, filter *handle.Filter, mp *mpool.MPool,
) (offset uint32, err error) {
	return blk.entry.GetBlockData().GetByFilter(ctx, blk.table.store.txn, filter, mp)
}

// newRelationBlockItOnSnap make a iterator on txn 's Objects of snapshot, exclude Object of workspace
// TODO: Objectit or tableit
func newRelationBlockItOnSnap(rel handle.Relation) *relBlockIt {
	it := new(relBlockIt)
	ObjectIt := rel.MakeObjectItOnSnap()
	if !ObjectIt.Valid() {
		it.err = ObjectIt.GetError()
		return it
	}
	obj := ObjectIt.GetObject()
	blockIt := obj.MakeBlockIt()
	for !blockIt.Valid() {
		ObjectIt.Next()
		if !ObjectIt.Valid() {
			it.err = ObjectIt.GetError()
			return it
		}
		obj.Close()
		obj = ObjectIt.GetObject()
		blockIt = obj.MakeBlockIt()
	}
	obj.Close()
	it.blockIt = blockIt
	it.ObjectIt = ObjectIt
	it.rel = rel
	it.err = blockIt.GetError()
	return it
}

// TODO: Objectit or tableit
func newRelationBlockIt(rel handle.Relation) *relBlockIt {
	it := new(relBlockIt)
	ObjectIt := rel.MakeObjectIt()
	if !ObjectIt.Valid() {
		it.err = ObjectIt.GetError()
		return it
	}
	obj := ObjectIt.GetObject()
	blockIt := obj.MakeBlockIt()
	for !blockIt.Valid() {
		ObjectIt.Next()
		if !ObjectIt.Valid() {
			it.err = ObjectIt.GetError()
			return it
		}
		obj.Close()
		obj = ObjectIt.GetObject()
		blockIt = obj.MakeBlockIt()
	}
	obj.Close()
	it.blockIt = blockIt
	it.ObjectIt = ObjectIt
	it.rel = rel
	it.err = blockIt.GetError()
	return it
}

func (it *relBlockIt) Close() error    { return nil }
func (it *relBlockIt) GetError() error { return it.err }
func (it *relBlockIt) Valid() bool {
	var err error
	if it.err != nil {
		return false
	}
	if it.ObjectIt == nil {
		return false
	}
	if !it.ObjectIt.Valid() {
		if err = it.ObjectIt.GetError(); err != nil {
			it.err = err
		}
		return false
	}
	if it.blockIt.Valid() {
		return true
	}

	if err = it.blockIt.GetError(); err != nil {
		it.err = err
	}
	if it.err != nil {
		return false
	}
	var obj handle.Object
	for {
		it.ObjectIt.Next()
		if !it.ObjectIt.Valid() {
			if err = it.ObjectIt.GetError(); err != nil {
				it.err = err
			}
			return false
		}
		if obj != nil {
			obj.Close()
		}
		obj = it.ObjectIt.GetObject()
		meta := obj.GetMeta().(*catalog.ObjectEntry)
		meta.RLock()
		cnt := meta.BlockCnt()
		meta.RUnlock()
		if cnt != 0 {
			break
		}
	}
	if obj != nil {
		obj.Close()
	}
	it.blockIt = obj.MakeBlockIt()
	if err = it.blockIt.GetError(); err != nil {
		it.err = err
	}
	return it.blockIt.Valid()
}

func (it *relBlockIt) GetBlock() handle.Block {
	return it.blockIt.GetBlock()
}

func (it *relBlockIt) Next() {
	it.blockIt.Next()
	if it.blockIt.Valid() {
		return
	}
	it.ObjectIt.Next()
	if !it.ObjectIt.Valid() {
		return
	}
	obj := it.ObjectIt.GetObject()
	it.blockIt = obj.MakeBlockIt()
	for !it.blockIt.Valid() {
		it.ObjectIt.Next()
		if !it.ObjectIt.Valid() {
			return
		}
		obj.Close()
		obj = it.ObjectIt.GetObject()
		it.blockIt = obj.MakeBlockIt()
	}
	obj.Close()
}
