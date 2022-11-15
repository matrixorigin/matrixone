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
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrDuplicateNode = moerr.NewInternalError("tae: duplicate node")
)

type txnEntries struct {
	entries []txnif.TxnEntry
	mask    *roaring.Bitmap
}

func newTxnEntries() *txnEntries {
	return &txnEntries{
		entries: make([]txnif.TxnEntry, 0),
		mask:    roaring.New(),
	}
}

func (entries *txnEntries) Len() int {
	return len(entries.entries)
}

func (entries *txnEntries) Append(entry txnif.TxnEntry) {
	entries.entries = append(entries.entries, entry)
}

func (entries *txnEntries) Delete(idx int) {
	entries.mask.Add(uint32(idx))
}

func (entries *txnEntries) IsDeleted(idx int) bool {
	return entries.mask.ContainsInt(idx)
}

func (entries *txnEntries) AnyDelete() bool {
	return !entries.mask.IsEmpty()
}

func (entries *txnEntries) Close() {
	entries.mask = nil
	entries.entries = nil
}

type deleteNode struct {
	txnif.DeleteNode
	idx int
}

func newDeleteNode(node txnif.DeleteNode, idx int) *deleteNode {
	return &deleteNode{
		DeleteNode: node,
		idx:        idx,
	}
}

type txnTable struct {
	store        *txnStore
	createEntry  txnif.TxnEntry
	dropEntry    txnif.TxnEntry
	localSegment *localSegment
	deleteNodes  map[common.ID]*deleteNode
	entry        *catalog.TableEntry
	schema       *catalog.Schema
	logs         []wal.LogEntry
	maxSegId     uint64
	maxBlkId     uint64

	txnEntries *txnEntries
	csnStart   uint32

	idx int
}

func newTxnTable(store *txnStore, entry *catalog.TableEntry) *txnTable {
	tbl := &txnTable{
		store:       store,
		entry:       entry,
		schema:      entry.GetSchema(),
		deleteNodes: make(map[common.ID]*deleteNode),
		logs:        make([]wal.LogEntry, 0),
		txnEntries:  newTxnEntries(),
	}
	return tbl
}

func (tbl *txnTable) PrePreareTransfer() (err error) {
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	return tbl.TransferDeletes(ts)
}

func (tbl *txnTable) TransferDeleteIntent(
	id *common.ID,
	row uint32) (changed bool, nid *common.ID, nrow uint32, err error) {
	pinned, err := tbl.store.transferTable.Pin(*id)
	if err != nil {
		err = nil
		return
	}
	defer pinned.Close()
	entry, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.SegmentID,
		id.BlockID)
	if err != nil {
		panic(err)
	}
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	if err = readWriteConfilictCheck(entry.MetaBaseEntry, ts); err == nil {
		return
	}
	err = nil
	nid = &common.ID{
		TableID: id.TableID,
	}
	rowID, ok := pinned.Item().Transfer(row)
	if !ok {
		err = moerr.NewTxnWWConflict()
		return
	}
	changed = true
	nid.SegmentID, nid.BlockID, nrow = model.DecodePhyAddrKey(rowID)
	return
}

func (tbl *txnTable) TransferDeletes(ts types.TS) (err error) {
	if tbl.store.transferTable == nil {
		return
	}
	if len(tbl.deleteNodes) == 0 {
		return
	}
	for id, node := range tbl.deleteNodes {
		// search the read set to check wether the delete node relevant
		// block was deleted.
		// if not deleted, go to next
		// if deleted, try to transfer the delete node
		if err = tbl.store.warChecker.checkOne(
			&id,
			ts,
		); err == nil {
			continue
		}

		// if the error is not a r-w conflict. something wrong really happened
		if !moerr.IsMoErrCode(err, moerr.ErrTxnRWConflict) {
			return
		}

		// try to transfer the delete node
		// here are some possible returns
		// nil: transferred successfully
		// ErrTxnRWConflict: the target block was also be compacted
		// ErrTxnWWConflict: w-w error
		if _, err = tbl.TransferDelete(&id, node); err != nil {
			return
		}
	}
	return
}

func (tbl *txnTable) recurTransferDelete(
	memo map[uint64]*common.PinnedItem[*model.TransferHashPage],
	page *model.TransferHashPage,
	id *common.ID,
	row uint32,
	depth int) (err error) {

	var page2 *common.PinnedItem[*model.TransferHashPage]

	rowID, ok := page.Transfer(row)
	if !ok {
		err = moerr.NewTxnWWConflict()
		msg := fmt.Sprintf("table-%d blk-%d delete row-%d depth-%d",
			id.TableID,
			id.BlockID,
			row,
			depth)
		logutil.Warnf("[ts=%s]TransferDelete: %v",
			tbl.store.txn.GetStartTS().ToString(),
			msg)
		return
	}
	segmentID, blockID, offset := model.DecodePhyAddrKey(rowID)
	newID := &common.ID{
		TableID:   id.TableID,
		SegmentID: segmentID,
		BlockID:   blockID,
	}
	if page2, ok = memo[blockID]; !ok {
		if page2, err = tbl.store.transferTable.Pin(*newID); err != nil {
			err = nil
		} else {
			memo[blockID] = page2
		}
	}
	if page2 != nil {
		return tbl.recurTransferDelete(
			memo,
			page2.Item(),
			newID,
			offset,
			depth+1)
	}
	if err = tbl.RangeDelete(newID, offset, offset, handle.DT_Normal); err != nil {
		return
	}
	logutil.Infof("depth-%d transfer delete from blk-%d row-%d to blk-%d row-%d",
		depth,
		id.BlockID,
		row,
		blockID,
		offset)
	return
}

func (tbl *txnTable) TransferDelete(id *common.ID, node *deleteNode) (transferred bool, err error) {
	memo := make(map[uint64]*common.PinnedItem[*model.TransferHashPage])
	logutil.Info("[Start]",
		common.AnyField("txn-start-ts", tbl.store.txn.GetStartTS().ToString()),
		common.OperationField("transfer-deletes"),
		common.OperandField(id.BlockString()))
	defer func() {
		logutil.Info("[End]",
			common.AnyField("txn-start-ts", tbl.store.txn.GetStartTS().ToString()),
			common.OperationField("transfer-deletes"),
			common.OperandField(id.BlockString()),
			common.ErrorField(err))
		for _, m := range memo {
			m.Close()
		}
	}()

	pinned, err := tbl.store.transferTable.Pin(*id)
	// cannot find a transferred record. maybe the transferred record was TTL'ed
	// here we can convert the error back to r-w conflict
	if err != nil {
		err = moerr.NewTxnRWConflict()
		return
	}
	memo[id.BlockID] = pinned

	rows := node.DeletedRows()
	// logutil.Infof("TransferDelete deletenode %s", node.DeleteNode.(*updates.DeleteNode).GeneralVerboseString())
	page := pinned.Item()
	depth := 0
	for _, row := range rows {
		if err = tbl.recurTransferDelete(memo, page, id, row, depth); err != nil {
			return
		}
	}

	// rollback transferred delete node. should not fail
	if err = node.PrepareRollback(); err != nil {
		panic(err)
	}
	if err = node.ApplyRollback(nil); err != nil {
		panic(err)
	}

	tbl.commitTransferDeleteNode(id, node)
	transferred = true
	return
}

func (tbl *txnTable) commitTransferDeleteNode(id *common.ID, node *deleteNode) {
	tbl.store.warChecker.Delete(id)
	tbl.txnEntries.Delete(node.idx)
	delete(tbl.deleteNodes, *id)
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
	for idx, txnEntry := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
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
		tbl.txnEntries.Append(txnEntry)
	}
	tbl.store.txn.GetMemo().AddSegment(tbl.entry.GetDB().GetID(), tbl.entry.ID, id)
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
	tbl.store.txn.GetMemo().AddSegment(tbl.entry.GetDB().ID, tbl.entry.ID, meta.ID)
	if is1PC {
		meta.Set1PC()
	}
	tbl.txnEntries.Append(meta)
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
	tbl.store.txn.GetMemo().AddBlock(tbl.entry.GetDB().ID, id.TableID, id.SegmentID, id.BlockID)
	if meta != nil {
		tbl.txnEntries.Append(meta)
	}
	return
}

func (tbl *txnTable) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	tbl.store.IncreateWriteCnt()
	tbl.txnEntries.Append(entry)
	for _, id := range readed {
		// warChecker skip non-block read
		if id.BlockID == 0 {
			continue
		}

		// record block into read set
		tbl.store.warChecker.InsertByID(
			tbl.entry.GetDB().ID,
			id.TableID,
			id.SegmentID,
			id.BlockID)
	}
	return
}

func (tbl *txnTable) GetBlock(id *common.ID) (blk handle.Block, err error) {
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.SegmentID,
		id.BlockID)
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
		factory = tbl.store.dataFactory.MakeBlockFactory()
	}
	meta, err := seg.CreateBlock(tbl.store.txn, state, factory)
	if err != nil {
		return
	}
	if is1PC {
		meta.Set1PC()
	}
	tbl.store.IncreateWriteCnt()
	id := meta.AsCommonID()
	tbl.store.txn.GetMemo().AddBlock(tbl.entry.GetDB().ID, id.TableID, id.SegmentID, id.BlockID)
	tbl.txnEntries.Append(meta)
	return buildBlock(tbl, meta), err
}

func (tbl *txnTable) SetCreateEntry(e txnif.TxnEntry) {
	if tbl.createEntry != nil {
		panic("logic error")
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddCatalogChange()
	tbl.createEntry = e
	tbl.txnEntries.Append(e)
}

func (tbl *txnTable) SetDropEntry(e txnif.TxnEntry) error {
	if tbl.dropEntry != nil {
		panic("logic error")
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddCatalogChange()
	tbl.dropEntry = e
	tbl.txnEntries.Append(e)
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
	tbl.txnEntries = nil
	return nil
}

func (tbl *txnTable) AddDeleteNode(id *common.ID, node txnif.DeleteNode) error {
	nid := *id
	u := tbl.deleteNodes[nid]
	if u != nil {
		return ErrDuplicateNode
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddBlock(tbl.entry.GetDB().ID, id.TableID, id.SegmentID, id.BlockID)
	tbl.deleteNodes[nid] = newDeleteNode(node, tbl.txnEntries.Len())
	tbl.txnEntries.Append(node)
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
	defer func() {
		if err == nil {
			return
		}
		// if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
		// 	moerr.NewTxnWriteConflict("table-%d blk-%d delete rows from %d to %d",
		// 		id.TableID,
		// 		id.BlockID,
		// 		start,
		// 		end)
		// }
		if err != nil {
			logutil.Infof("[ts=%s]: table-%d blk-%d delete rows from %d to %d %v",
				tbl.store.txn.GetStartTS().ToString(),
				id.TableID,
				id.BlockID,
				start,
				end,
				err)
		}
	}()
	if isLocalSegment(id) {
		err = tbl.RangeDeleteLocalRows(start, end)
		return
	}
	node := tbl.deleteNodes[*id]
	if node != nil {
		// TODO: refactor
		chain := node.GetChain().(*updates.DeleteChain)
		mvcc := chain.GetController()
		mvcc.Lock()
		if err = mvcc.CheckNotDeleted(start, end, tbl.store.txn.GetStartTS()); err == nil {
			node.RangeDeleteLocked(start, end)
		}
		mvcc.Unlock()
		if err != nil {
			tbl.store.warChecker.Insert(mvcc.GetEntry())
		}
		return
	}
	blk, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID, id.SegmentID,
		id.BlockID)
	if err != nil {
		return
	}
	blkData := blk.GetBlockData()
	node2, err := blkData.RangeDelete(tbl.store.txn, start, end, dt)
	if err == nil {
		if err = tbl.AddDeleteNode(id, node2); err != nil {
			return
		}
		tbl.store.warChecker.Insert(blk)
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
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.SegmentID,
		id.BlockID)
	if err != nil {
		panic(err)
	}
	block := meta.GetBlockData()
	return block.GetValue(tbl.store.txn, int(row), int(col))
}

func (tbl *txnTable) UpdateMetaLoc(id *common.ID, metaloc string) (err error) {
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.SegmentID,
		id.BlockID)
	if err != nil {
		panic(err)
	}
	isNewNode, err := meta.UpdateMetaLoc(tbl.store.txn, metaloc)
	if err != nil {
		return
	}
	if isNewNode {
		tbl.txnEntries.Append(meta)
	}
	return
}

func (tbl *txnTable) UpdateDeltaLoc(id *common.ID, deltaloc string) (err error) {
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.SegmentID,
		id.BlockID)
	if err != nil {
		panic(err)
	}
	isNewNode, err := meta.UpdateDeltaLoc(tbl.store.txn, deltaloc)
	if err != nil {
		return
	}
	if isNewNode {
		tbl.txnEntries.Append(meta)
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
	err = tbl.PreCommitDedup(pks, true)
	return
}

func (tbl *txnTable) Dedup(keys containers.Vector) (err error) {
	h := newRelation(tbl)
	it := newRelationBlockIt(h)
	for it.Valid() {
		blk := it.GetBlock().GetMeta().(*catalog.BlockEntry)
		blkData := blk.GetBlockData()
		if blkData == nil {
			it.Next()
			continue
		}
		var rowmask *roaring.Bitmap
		if len(tbl.deleteNodes) > 0 {
			fp := blk.AsCommonID()
			deleteNode := tbl.deleteNodes[*fp]
			if deleteNode != nil {
				rowmask = deleteNode.GetRowMaskRefLocked()
			}
		}
		if err = blkData.BatchDedup(tbl.store.txn, keys, rowmask); err != nil {
			// logutil.Infof("%s, %s, %v", blk.String(), rowmask, err)
			return
		}
		it.Next()
	}
	return
}

func (tbl *txnTable) PreCommitDedup(pks containers.Vector, preCommit bool) (err error) {
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
			shouldSkip := seg.HasDropCommittedLocked() || seg.IsCreatingOrAborted()
			seg.RUnlock()
			if shouldSkip {
				segIt.Next()
				continue
			}
		}
		segData := seg.GetSegmentData()
		// TODO: Add a new batch dedup method later
		if err = segData.BatchDedup(tbl.store.txn, pks); moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			return
		}
		if err == nil {
			segIt.Next()
			continue
		}
		var shouldSkip bool
		err = nil
		blkIt := seg.MakeBlockIt(false)
		for blkIt.Valid() {
			blk := blkIt.Get().GetPayload()
			if preCommit && blk.GetID() < tbl.maxBlkId {
				return
			}
			{
				blk.RLock()
				if preCommit {
					shouldSkip = blk.HasDropCommittedLocked() || blk.IsCreatingOrAborted()
				} else {
					shouldSkip = blk.HasDropCommittedLocked() || !blk.HasCommittedNode()
				}
				blk.RUnlock()
				if shouldSkip {
					blkIt.Next()
					continue
				}
			}
			blkData := blk.GetBlockData()
			var rowmask *roaring.Bitmap
			if len(tbl.deleteNodes) > 0 {
				if tbl.store.warChecker.HasConflict(blk.ID) {
					continue
				}
				fp := blk.AsCommonID()
				deleteNode := tbl.deleteNodes[*fp]
				if deleteNode != nil {
					rowmask = deleteNode.GetRowMaskRefLocked()
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
	if err = index.BatchInsert(
		tbl.schema.GetSingleSortKey().Name,
		key,
		0,
		key.Length(),
		0,
		true); err != nil {
		return
	}

	if tbl.localSegment != nil {
		if err = tbl.localSegment.BatchDedup(key); err != nil {
			return
		}
	}

	err = tbl.Dedup(key)
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
	for idx, txnEntry := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
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
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
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
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
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
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
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
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
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
