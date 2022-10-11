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
	"fmt"

	"github.com/RoaringBitmap/roaring"

	// "github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrDuplicateNode = moerr.NewInternalError("tae: duplicate node")
)

type txnTable struct {
	store        *txnStore
	createEntry  txnif.TxnEntry
	dropEntry    txnif.TxnEntry
	localSegment *localSegment
	deleteNodes  map[common.ID]txnif.DeleteNode
	entry        *catalog.TableEntry
	schema       *catalog.Schema
	logs         []wal.LogEntry
	maxSegId     uint64
	maxBlkId     uint64

	txnEntries []txnif.TxnEntry
	csnStart   uint32

	idx int
}

func newTxnTable(store *txnStore, entry *catalog.TableEntry) *txnTable {
	tbl := &txnTable{
		store:       store,
		entry:       entry,
		schema:      entry.GetSchema(),
		deleteNodes: make(map[common.ID]txnif.DeleteNode),
		logs:        make([]wal.LogEntry, 0),
		txnEntries:  make([]txnif.TxnEntry, 0),
	}
	return tbl
}

func (tbl *txnTable) LogSegmentID(sid uint64) {
	if tbl.maxSegId < sid {
		tbl.maxSegId = sid
	}
}

func (tbl *txnTable) LogBlockID(bid uint64) {
	if tbl.maxBlkId < bid {
		tbl.maxBlkId = bid
	}
}

func (tbl *txnTable) WaitSynced() {
	for _, e := range tbl.logs {
		if err := e.WaitDone(); err != nil {
			panic(err)
		}
		e.Free()
	}
}

func (tbl *txnTable) CollectCmd(cmdMgr *commandManager) (err error) {
	tbl.csnStart = uint32(cmdMgr.GetCSN())
	for _, txnEntry := range tbl.txnEntries {
		csn := cmdMgr.GetCSN()
		cmd, err := txnEntry.MakeCommand(csn)
		// logutil.Infof("%d-%d",csn,cmd.GetType())
		if err != nil {
			return err
		}
		if cmd == nil {
			panic(txnEntry)
		}
		cmdMgr.AddCmd(cmd)
	}
	if tbl.localSegment != nil {
		if err = tbl.localSegment.CollectCmd(cmdMgr); err != nil {
			return
		}
	}
	return
}

func (tbl *txnTable) GetSegment(id uint64) (seg handle.Segment, err error) {
	var meta *catalog.SegmentEntry
	if meta, err = tbl.entry.GetSegmentByID(id); err != nil {
		return
	}
	var ok bool
	meta.RLock()
	ok, err = meta.IsVisible(tbl.store.txn.GetStartTS(), meta.RWMutex)
	meta.RUnlock()
	if err != nil {
		return
	}
	if !ok {
		err = moerr.NewNotFound()
		return
	}
	seg = newSegment(tbl, meta)
	return
}

func (tbl *txnTable) SoftDeleteSegment(id uint64) (err error) {
	txnEntry, err := tbl.entry.DropSegmentEntry(id, tbl.store.txn)
	if err != nil {
		return
	}
	tbl.store.IncreateWriteCnt()
	if txnEntry != nil {
		tbl.txnEntries = append(tbl.txnEntries, txnEntry)
	}
	tbl.store.dirtyMemo.recordSeg(tbl.entry.GetDB().GetID(), tbl.entry.ID, id)
	tbl.store.warChecker.ReadTable(tbl.entry.GetDB().ID, tbl.entry.AsCommonID())
	return
}

func (tbl *txnTable) CreateSegment(is1PC bool) (seg handle.Segment, err error) {
	return tbl.createSegment(catalog.ES_Appendable, is1PC)
}

func (tbl *txnTable) CreateNonAppendableSegment() (seg handle.Segment, err error) {
	return tbl.createSegment(catalog.ES_NotAppendable, false)
}

func (tbl *txnTable) createSegment(state catalog.EntryState, is1PC bool) (seg handle.Segment, err error) {
	var meta *catalog.SegmentEntry
	var factory catalog.SegmentDataFactory
	if tbl.store.dataFactory != nil {
		factory = tbl.store.dataFactory.MakeSegmentFactory()
	}
	if meta, err = tbl.entry.CreateSegment(tbl.store.txn, state, factory); err != nil {
		return
	}
	seg = newSegment(tbl, meta)
	tbl.store.IncreateWriteCnt()
	tbl.store.dirtyMemo.recordSeg(tbl.entry.GetDB().ID, tbl.entry.ID, meta.ID)
	if is1PC {
		meta.Set1PC()
	}
	tbl.txnEntries = append(tbl.txnEntries, meta)
	tbl.store.warChecker.ReadTable(tbl.entry.GetDB().ID, meta.GetTable().AsCommonID())
	return
}

func (tbl *txnTable) SoftDeleteBlock(id *common.ID) (err error) {
	var seg *catalog.SegmentEntry
	if seg, err = tbl.entry.GetSegmentByID(id.SegmentID); err != nil {
		return
	}
	meta, err := seg.DropBlockEntry(id.BlockID, tbl.store.txn)
	if err != nil {
		return
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.dirtyMemo.recordBlk(tbl.entry.GetDB().ID, id)
	if meta != nil {
		tbl.txnEntries = append(tbl.txnEntries, meta)
	}
	tbl.store.warChecker.ReadSegment(tbl.entry.GetDB().ID, seg.AsCommonID())
	return
}

func (tbl *txnTable) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	tbl.store.IncreateWriteCnt()
	tbl.txnEntries = append(tbl.txnEntries, entry)
	for _, id := range readed {
		tbl.store.warChecker.Read(tbl.entry.GetDB().ID, id)
	}
	return
}

func (tbl *txnTable) GetBlock(id *common.ID) (blk handle.Block, err error) {
	var seg *catalog.SegmentEntry
	if seg, err = tbl.entry.GetSegmentByID(id.SegmentID); err != nil {
		return
	}
	meta, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		return
	}
	blk = buildBlock(tbl, meta)
	return
}

func (tbl *txnTable) CreateNonAppendableBlock(sid uint64) (blk handle.Block, err error) {
	return tbl.createBlock(sid, catalog.ES_NotAppendable, false)
}

func (tbl *txnTable) CreateBlock(sid uint64, is1PC bool) (blk handle.Block, err error) {
	return tbl.createBlock(sid, catalog.ES_Appendable, is1PC)
}

func (tbl *txnTable) createBlock(sid uint64, state catalog.EntryState, is1PC bool) (blk handle.Block, err error) {
	var seg *catalog.SegmentEntry
	if seg, err = tbl.entry.GetSegmentByID(sid); err != nil {
		return
	}
	if !seg.IsAppendable() && state == catalog.ES_Appendable {
		err = moerr.NewInternalError("not appendable")
		return
	}
	var factory catalog.BlockDataFactory
	if tbl.store.dataFactory != nil {
		segData := seg.GetSegmentData()
		factory = tbl.store.dataFactory.MakeBlockFactory(segData.GetSegmentFile())
	}
	meta, err := seg.CreateBlock(tbl.store.txn, state, factory)
	if err != nil {
		return
	}
	if is1PC {
		meta.Set1PC()
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.dirtyMemo.recordBlk(tbl.entry.GetDB().ID, meta.AsCommonID())
	tbl.txnEntries = append(tbl.txnEntries, meta)
	tbl.store.warChecker.ReadSegment(tbl.entry.GetDB().ID, seg.AsCommonID())
	return buildBlock(tbl, meta), err
}

func (tbl *txnTable) SetCreateEntry(e txnif.TxnEntry) {
	if tbl.createEntry != nil {
		panic("logic error")
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.dirtyMemo.recordCatalogChange()
	tbl.createEntry = e
	tbl.txnEntries = append(tbl.txnEntries, e)
	tbl.store.warChecker.ReadDB(tbl.entry.GetDB().GetID())
}

func (tbl *txnTable) SetDropEntry(e txnif.TxnEntry) error {
	if tbl.dropEntry != nil {
		panic("logic error")
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.dirtyMemo.recordCatalogChange()
	tbl.dropEntry = e
	tbl.txnEntries = append(tbl.txnEntries, e)
	tbl.store.warChecker.ReadDB(tbl.entry.GetDB().GetID())
	return nil
}

func (tbl *txnTable) IsDeleted() bool {
	return tbl.dropEntry != nil
}

func (tbl *txnTable) GetSchema() *catalog.Schema {
	return tbl.schema
}

func (tbl *txnTable) GetMeta() *catalog.TableEntry {
	return tbl.entry
}

func (tbl *txnTable) GetID() uint64 {
	return tbl.entry.GetID()
}

func (tbl *txnTable) Close() error {
	var err error
	if tbl.localSegment != nil {
		if err = tbl.localSegment.Close(); err != nil {
			return err
		}
		tbl.localSegment = nil
	}
	tbl.deleteNodes = nil
	tbl.logs = nil
	return nil
}

func (tbl *txnTable) AddDeleteNode(id *common.ID, node txnif.DeleteNode) error {
	nid := *id
	u := tbl.deleteNodes[nid]
	if u != nil {
		return ErrDuplicateNode
	}
	tbl.deleteNodes[nid] = node
	tbl.store.IncreateWriteCnt()
	tbl.store.dirtyMemo.recordBlk(tbl.entry.GetDB().ID, id)
	tbl.txnEntries = append(tbl.txnEntries, node)
	return nil
}

func (tbl *txnTable) Append(data *containers.Batch) (err error) {
	if tbl.schema.HasPK() {
		if err = tbl.DoBatchDedup(data.Vecs[tbl.schema.GetSingleSortKeyIdx()]); err != nil {
			return
		}
	}
	if tbl.localSegment == nil {
		tbl.localSegment = newLocalSegment(tbl)
	}
	return tbl.localSegment.Append(data)
}

func (tbl *txnTable) RangeDeleteLocalRows(start, end uint32) (err error) {
	if tbl.localSegment != nil {
		err = tbl.localSegment.RangeDelete(start, end)
	}
	return
}

func (tbl *txnTable) LocalDeletesToString() string {
	s := fmt.Sprintf("<txnTable-%d>[LocalDeletes]:\n", tbl.GetID())
	if tbl.localSegment != nil {
		s = fmt.Sprintf("%s%s", s, tbl.localSegment.DeletesToString())
	}
	return s
}

func (tbl *txnTable) IsLocalDeleted(row uint32) bool {
	if tbl.localSegment == nil {
		return false
	}
	return tbl.localSegment.IsDeleted(row)
}

func (tbl *txnTable) RangeDelete(id *common.ID, start, end uint32, dt handle.DeleteType) (err error) {
	if isLocalSegment(id) {
		return tbl.RangeDeleteLocalRows(start, end)
	}
	node := tbl.deleteNodes[*id]
	if node != nil {
		chain := node.GetChain().(*updates.DeleteChain)
		mvcc := chain.GetController()
		mvcc.Lock()
		if err = mvcc.CheckNotDeleted(start, end, tbl.store.txn.GetStartTS()); err == nil {
			node.RangeDeleteLocked(start, end)
		}
		mvcc.Unlock()
		if err != nil {
			seg, _ := tbl.entry.GetSegmentByID(id.SegmentID)
			blk, _ := seg.GetBlockEntryByID(id.BlockID)
			tbl.store.warChecker.ReadBlock(tbl.entry.GetDB().ID, blk.AsCommonID())
		}
		return
	}
	seg, err := tbl.entry.GetSegmentByID(id.SegmentID)
	if err != nil {
		return
	}
	blk, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		return
	}
	blkData := blk.GetBlockData()
	node2, err := blkData.RangeDelete(tbl.store.txn, start, end, dt)
	if err == nil {
		id := blk.AsCommonID()
		if err = tbl.AddDeleteNode(id, node2); err != nil {
			return
		}
		tbl.store.warChecker.ReadBlock(tbl.entry.GetDB().ID, id)
	}
	return
}

func (tbl *txnTable) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	if tbl.localSegment != nil {
		id, offset, err = tbl.localSegment.GetByFilter(filter)
		if err == nil {
			return
		}
		err = nil
	}
	h := newRelation(tbl)
	blockIt := h.MakeBlockIt()
	for blockIt.Valid() {
		h := blockIt.GetBlock()
		if h.IsUncommitted() {
			blockIt.Next()
			continue
		}
		offset, err = h.GetByFilter(filter)
		// block := h.GetMeta().(*catalog.BlockEntry).GetBlockData()
		// offset, err = block.GetByFilter(tbl.store.txn, filter)
		if err == nil {
			id = h.Fingerprint()
			break
		}
		blockIt.Next()
	}
	if err == nil && id == nil {
		err = moerr.NewNotFound()
	}
	return
}

func (tbl *txnTable) GetLocalValue(row uint32, col uint16) (v any, err error) {
	if tbl.localSegment == nil {
		return
	}
	return tbl.localSegment.GetValue(row, col)
}

func (tbl *txnTable) GetValue(id *common.ID, row uint32, col uint16) (v any, err error) {
	if isLocalSegment(id) {
		return tbl.localSegment.GetValue(row, col)
	}
	segMeta, err := tbl.entry.GetSegmentByID(id.SegmentID)
	if err != nil {
		panic(err)
	}
	meta, err := segMeta.GetBlockEntryByID(id.BlockID)
	if err != nil {
		panic(err)
	}
	block := meta.GetBlockData()
	return block.GetValue(tbl.store.txn, int(row), int(col))
}

func (tbl *txnTable) UpdateMetaLoc(id *common.ID, metaloc string) (err error) {
	segMeta, err := tbl.entry.GetSegmentByID(id.SegmentID)
	if err != nil {
		panic(err)
	}
	meta, err := segMeta.GetBlockEntryByID(id.BlockID)
	if err != nil {
		panic(err)
	}
	isNewNode, err := meta.UpdateMetaLoc(tbl.store.txn, metaloc)
	if err != nil {
		return
	}
	if isNewNode {
		tbl.txnEntries = append(tbl.txnEntries, meta)
	}
	return
}

func (tbl *txnTable) UpdateDeltaLoc(id *common.ID, deltaloc string) (err error) {
	segMeta, err := tbl.entry.GetSegmentByID(id.SegmentID)
	if err != nil {
		panic(err)
	}
	meta, err := segMeta.GetBlockEntryByID(id.BlockID)
	if err != nil {
		panic(err)
	}
	isNewNode, err := meta.UpdateDeltaLoc(tbl.store.txn, deltaloc)
	if err != nil {
		return
	}
	if isNewNode {
		tbl.txnEntries = append(tbl.txnEntries, meta)
	}
	return
}

func (tbl *txnTable) UncommittedRows() uint32 {
	if tbl.localSegment == nil {
		return 0
	}
	return tbl.localSegment.Rows()
}
func (tbl *txnTable) NeedRollback() bool {
	return tbl.createEntry != nil && tbl.dropEntry != nil
}

// PrePrepareDedup do deduplication check for 1PC Commit or 2PC Prepare
func (tbl *txnTable) PrePrepareDedup() (err error) {
	if tbl.localSegment == nil || !tbl.schema.HasPK() {
		return
	}
	pks := tbl.localSegment.GetPKColumn()
	defer pks.Close()
	err = tbl.DoDedup(pks, true)
	return
}

func (tbl *txnTable) DoDedup(pks containers.Vector, preCommit bool) (err error) {
	segIt := tbl.entry.MakeSegmentIt(false)
	for segIt.Valid() {
		seg := segIt.Get().GetPayload()
		if preCommit && seg.GetID() < tbl.maxSegId {
			return
		}
		{
			seg.RLock()
			needwait, txnToWait := seg.NeedWaitCommitting(tbl.store.txn.GetStartTS())
			if needwait {
				seg.RUnlock()
				txnToWait.GetTxnState(true)
				seg.RLock()
			}
			invalid := seg.HasDropCommittedLocked() || seg.IsCreating()
			seg.RUnlock()
			if invalid {
				segIt.Next()
				continue
			}
		}
		segData := seg.GetSegmentData()
		// TODO: Add a new batch dedup method later
		if err = segData.BatchDedup(tbl.store.txn, pks); moerr.IsMoErrCode(err, moerr.ErrDuplicate) {
			return
		}
		if err == nil {
			segIt.Next()
			continue
		}
		err = nil
		blkIt := seg.MakeBlockIt(false)
		for blkIt.Valid() {
			blk := blkIt.Get().GetPayload()
			if preCommit && blk.GetID() < tbl.maxBlkId {
				return
			}
			{
				blk.RLock()
				invalid := blk.HasDropCommittedLocked() || blk.IsCreating()
				blk.RUnlock()
				if invalid {
					blkIt.Next()
					continue
				}
			}
			// logutil.Infof("%s: %d-%d, %d-%d: %s", tbl.txn.String(), tbl.maxSegId, tbl.maxBlkId, seg.GetID(), blk.GetID(), pks.String())
			blkData := blk.GetBlockData()
			var rowmask *roaring.Bitmap
			if len(tbl.deleteNodes) > 0 {
				fp := blk.AsCommonID()
				dn := tbl.deleteNodes[*fp]
				if dn != nil {
					rowmask = dn.GetRowMaskRefLocked()
				}
			}
			if err = blkData.BatchDedup(tbl.store.txn, pks, rowmask); err != nil {
				return
			}
			blkIt.Next()
		}
		segIt.Next()
	}
	return
}

func (tbl *txnTable) DoBatchDedup(key containers.Vector) (err error) {
	index := NewSimpleTableIndex()
	if err = index.BatchInsert(key, 0, key.Length(), 0, true); err != nil {
		return
	}

	if tbl.localSegment != nil {
		if err = tbl.localSegment.BatchDedup(key); err != nil {
			return
		}
	}

	err = tbl.DoDedup(key, false)
	return
}

func (tbl *txnTable) BatchDedupLocal(bat *containers.Batch) (err error) {
	if tbl.localSegment == nil || !tbl.schema.HasPK() {
		return
	}
	err = tbl.localSegment.BatchDedup(bat.Vecs[tbl.schema.GetSingleSortKeyIdx()])
	return
}

func (tbl *txnTable) PrepareRollback() (err error) {
	for _, txnEntry := range tbl.txnEntries {
		if err = txnEntry.PrepareRollback(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) ApplyAppend() (err error) {
	if tbl.localSegment != nil {
		err = tbl.localSegment.ApplyAppend()
	}
	return
}

func (tbl *txnTable) PrePrepare() (err error) {
	if tbl.localSegment != nil {
		err = tbl.localSegment.PrepareApply()
	}
	return
}

func (tbl *txnTable) PrepareCommit() (err error) {
	for _, node := range tbl.txnEntries {
		if err = node.PrepareCommit(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) PreApplyCommit() (err error) {
	return tbl.ApplyAppend()
}

func (tbl *txnTable) ApplyCommit() (err error) {
	csn := tbl.csnStart
	for _, node := range tbl.txnEntries {
		if node.Is1PC() {
			continue
		}
		if err = node.ApplyCommit(tbl.store.cmdMgr.MakeLogIndex(csn)); err != nil {
			break
		}
		csn++
	}
	return
}

func (tbl *txnTable) Apply1PCCommit() (err error) {
	for _, node := range tbl.txnEntries {
		if !node.Is1PC() {
			continue
		}
		if err = node.ApplyCommit(tbl.store.cmdMgr.MakeLogIndex(tbl.csnStart)); err != nil {
			break
		}
		tbl.csnStart++
	}
	return
}
func (tbl *txnTable) ApplyRollback() (err error) {
	csn := tbl.csnStart
	for _, node := range tbl.txnEntries {
		if node.Is1PC() {
			continue
		}
		if err = node.ApplyRollback(tbl.store.cmdMgr.MakeLogIndex(csn)); err != nil {
			break
		}
		csn++
	}
	return
}
