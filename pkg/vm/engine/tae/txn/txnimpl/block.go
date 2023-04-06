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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnBlock struct {
	*txnbase.TxnBlock
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
	rel       handle.Relation
	segmentIt handle.SegmentIt
	blockIt   handle.BlockIt
	err       error
}

func newBlockIt(table *txnTable, meta *catalog.SegmentEntry) *blockIt {
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
	if meta.GetSegment().GetTable().GetDB().IsSystemDB() {
		return newSysBlock(table, meta)
	}
	return newBlock(table, meta)
}

func newBlock(table *txnTable, meta *catalog.BlockEntry) *txnBlock {
	blk := &txnBlock{
		TxnBlock: &txnbase.TxnBlock{
			Txn: table.store.txn,
		},
		entry:         meta,
		table:         table,
		isUncommitted: meta.GetSegment().IsLocal,
	}
	return blk
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
	return blk.entry.GetSegment().GetTable().GetDB().ID
}

func (blk *txnBlock) RangeDelete(start, end uint32, dt handle.DeleteType) (err error) {
	return blk.Txn.GetStore().RangeDelete(blk.getDBID(), blk.entry.AsCommonID(), start, end, dt)
}

func (blk *txnBlock) GetMetaLoc() (metaloc string) {
	return blk.entry.GetVisibleMetaLoc(blk.Txn)
}
func (blk *txnBlock) GetDeltaLoc() (deltaloc string) {
	return blk.entry.GetVisibleDeltaLoc(blk.Txn)
}
func (blk *txnBlock) UpdateMetaLoc(metaloc string) (err error) {
	blkID := blk.Fingerprint()
	dbid := blk.GetMeta().(*catalog.BlockEntry).GetSegment().GetTable().GetDB().ID
	err = blk.Txn.GetStore().UpdateMetaLoc(dbid, blkID, metaloc)
	return
}

func (blk *txnBlock) UpdateDeltaLoc(deltaloc string) (err error) {
	blkID := blk.Fingerprint()
	dbid := blk.GetMeta().(*catalog.BlockEntry).GetSegment().GetTable().GetDB().ID
	err = blk.Txn.GetStore().UpdateDeltaLoc(dbid, blkID, deltaloc)
	return
}

// TODO: temp use coarse rows
func (blk *txnBlock) Rows() int {
	if blk.isUncommitted {
		return blk.table.localSegment.GetBlockRows(blk.entry)
	}
	return blk.entry.GetBlockData().Rows()
}

func (blk *txnBlock) GetColumnDataByIds(colIdxes []int) (*model.BlockView, error) {
	if blk.isUncommitted {
		return blk.table.localSegment.GetColumnDataByIds(blk.entry, colIdxes)
	}
	return blk.entry.GetBlockData().GetColumnDataByIds(blk.Txn, colIdxes)
}

func (blk *txnBlock) GetColumnDataByNames(attrs []string) (*model.BlockView, error) {
	if blk.isUncommitted {
		attrIds := make([]int, len(attrs))
		for i, attr := range attrs {
			attrIds[i] = blk.table.entry.GetSchema().GetColIdx(attr)
		}
		return blk.table.localSegment.GetColumnDataByIds(blk.entry, attrIds)
	}
	return blk.entry.GetBlockData().GetColumnDataByNames(blk.Txn, attrs)
}

func (blk *txnBlock) GetColumnDataById(colIdx int) (*model.ColumnView, error) {
	if blk.isUncommitted {
		return blk.table.localSegment.GetColumnDataById(blk.entry, colIdx)
	}
	return blk.entry.GetBlockData().GetColumnDataById(blk.Txn, colIdx)
}

func (blk *txnBlock) Prefetch(idxes []uint16) error {
	if blk.isUncommitted {
		return blk.table.localSegment.Prefetch(blk.entry, idxes)
	}
	return blk.entry.GetBlockData().Prefetch(idxes)
}

func (blk *txnBlock) GetColumnDataByName(attr string) (*model.ColumnView, error) {
	if blk.isUncommitted {
		attrId := blk.table.entry.GetSchema().GetColIdx(attr)
		return blk.table.localSegment.GetColumnDataById(blk.entry, attrId)
	}
	return blk.entry.GetBlockData().GetColumnDataByName(blk.Txn, attr)
}

func (blk *txnBlock) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return blk.Txn.GetStore().LogTxnEntry(blk.getDBID(), blk.entry.GetSegment().GetTable().ID, entry, readed)
}

func (blk *txnBlock) GetSegment() (seg handle.Segment) {
	seg = newSegment(blk.table, blk.entry.GetSegment())
	return
}

func (blk *txnBlock) GetByFilter(filter *handle.Filter) (offset uint32, err error) {
	return blk.entry.GetBlockData().GetByFilter(blk.table.store.txn, filter)
}

// newRelationBlockItOnSnap make a iterator on txn 's segments of snapshot, exclude segment of workspace
// TODO: segmentit or tableit
func newRelationBlockItOnSnap(rel handle.Relation) *relBlockIt {
	it := new(relBlockIt)
	segmentIt := rel.MakeSegmentItOnSnap()
	if !segmentIt.Valid() {
		it.err = segmentIt.GetError()
		return it
	}
	seg := segmentIt.GetSegment()
	blockIt := seg.MakeBlockIt()
	for !blockIt.Valid() {
		segmentIt.Next()
		if !segmentIt.Valid() {
			it.err = segmentIt.GetError()
			return it
		}
		seg = segmentIt.GetSegment()
		blockIt = seg.MakeBlockIt()
	}
	it.blockIt = blockIt
	it.segmentIt = segmentIt
	it.rel = rel
	it.err = blockIt.GetError()
	return it
}

// TODO: segmentit or tableit
func newRelationBlockIt(rel handle.Relation) *relBlockIt {
	it := new(relBlockIt)
	segmentIt := rel.MakeSegmentIt()
	if !segmentIt.Valid() {
		it.err = segmentIt.GetError()
		return it
	}
	seg := segmentIt.GetSegment()
	blockIt := seg.MakeBlockIt()
	for !blockIt.Valid() {
		segmentIt.Next()
		if !segmentIt.Valid() {
			it.err = segmentIt.GetError()
			return it
		}
		seg = segmentIt.GetSegment()
		blockIt = seg.MakeBlockIt()
	}
	it.blockIt = blockIt
	it.segmentIt = segmentIt
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
	if it.segmentIt == nil {
		return false
	}
	if !it.segmentIt.Valid() {
		if err = it.segmentIt.GetError(); err != nil {
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
	var seg handle.Segment
	for {
		it.segmentIt.Next()
		if !it.segmentIt.Valid() {
			if err = it.segmentIt.GetError(); err != nil {
				it.err = err
			}
			return false
		}
		seg = it.segmentIt.GetSegment()
		meta := seg.GetMeta().(*catalog.SegmentEntry)
		meta.RLock()
		cnt := meta.BlockCnt()
		meta.RUnlock()
		if cnt != 0 {
			break
		}
	}
	it.blockIt = seg.MakeBlockIt()
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
	it.segmentIt.Next()
	if !it.segmentIt.Valid() {
		return
	}
	seg := it.segmentIt.GetSegment()
	it.blockIt = seg.MakeBlockIt()
	for !it.blockIt.Valid() {
		it.segmentIt.Next()
		if !it.segmentIt.Valid() {
			return
		}
		seg := it.segmentIt.GetSegment()
		it.blockIt = seg.MakeBlockIt()
	}
}
